//! Receiver task for block-first lossless sessions.
//!
//! The receiver accepts a manifest, records completed plain blocks locally, and
//! optionally accumulates FEC symbols until a block can be decoded. After
//! `SourceDone(round_id)`, plain mode emits end-of-round `Need` feedback while
//! FEC mode emits one aggregate `Need` describing either completion or the
//! remaining per-block deficits for the next retransmit round.

mod cloudcast;
mod fec;
mod mettle_carousel;
mod plain;

use std::collections::{BTreeSet, VecDeque};
use std::future::pending;
use std::io::{self, Seek, SeekFrom, Write};
use std::ops::Bound::{Excluded, Unbounded};
use std::sync::Arc;

use tokio::sync::{mpsc, oneshot};
use tokio::time::Instant;
use tracing::{debug, info, warn};

use nextmini_messages::lossless_session::{
    self, BlockAck, CompletedBlockRange, FecFeedbackMode, FecScheme, LosslessSessionControl,
    LosslessSessionFecMode, LosslessSessionManifest, LosslessSessionMode, MissingBlockRange,
    NeedReport,
};

use crate::node::processor::ProcessorHandle;
use crate::node::session::api::{
    CompletedReceiverReplay, InboundFrame, LosslessRuntimeMessage, SessionId, SessionOutcome,
};
use crate::node::session::control;
use crate::node::session::metrics::SessionMetrics;
use crate::node::session::plan::{BlockPlan, ObjectSymbolPlan, SymbolGeometry};
use crate::node::session::runtime::{ReceiverConfig, TransportRoute};
use crate::node::session::timing;

use self::cloudcast::CloudcastReceiver;
use self::fec::FecReceiver;
use self::mettle_carousel::MettleCarouselReceiver;
use self::plain::PlainReceiver;

/// Run one receiver session until the transfer is complete or the channel closes.
#[allow(dead_code)]
pub async fn run(
    cfg: ReceiverConfig,
    data_rx: mpsc::Receiver<InboundFrame>,
    processors: ProcessorHandle,
) {
    let (_control_tx, control_rx) = mpsc::channel(1);
    let _ = run_with_runtime(cfg, control_rx, data_rx, processors, None).await;
}

pub(super) async fn run_with_runtime(
    cfg: ReceiverConfig,
    control_rx: mpsc::Receiver<InboundFrame>,
    data_rx: mpsc::Receiver<InboundFrame>,
    processors: ProcessorHandle,
    runtime_sender: Option<mpsc::Sender<LosslessRuntimeMessage>>,
) -> SessionOutcome {
    run_with_runtime_and_metrics(
        cfg,
        control_rx,
        data_rx,
        processors,
        runtime_sender,
        Arc::new(SessionMetrics::default()),
    )
    .await
}

/// Deterministic receiver test hook with caller-owned control/data inboxes and observer.
#[allow(dead_code)] // consumed by external and path-including conformance tests
pub async fn run_observed(
    cfg: ReceiverConfig,
    control_rx: mpsc::Receiver<InboundFrame>,
    data_rx: mpsc::Receiver<InboundFrame>,
    processors: ProcessorHandle,
    metrics: Arc<SessionMetrics>,
) -> SessionOutcome {
    run_with_runtime_and_metrics(cfg, control_rx, data_rx, processors, None, metrics).await
}

async fn run_with_runtime_and_metrics(
    cfg: ReceiverConfig,
    mut control_rx: mpsc::Receiver<InboundFrame>,
    mut data_rx: mpsc::Receiver<InboundFrame>,
    processors: ProcessorHandle,
    runtime_sender: Option<mpsc::Sender<LosslessRuntimeMessage>>,
    metrics: Arc<SessionMetrics>,
) -> SessionOutcome {
    let mut receiver = SessionReceiver::new_with_metrics(cfg, processors, metrics);
    receiver
        .run(&mut control_rx, &mut data_rx, runtime_sender)
        .await
}

/// Stateful receiver loop shared by plain and FEC transfer modes.
struct SessionReceiver {
    shared: ReceiverShared,
    mode: Option<ReceiverMode>,
    lifecycle: ReceiverLifecycle,
    passive_complete_deadline: Option<Instant>,
    pending_control_frames: VecDeque<InboundFrame>,
    carousel_ack: Option<CarouselAckState>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ReceiverLifecycle {
    Active,
    PassiveComplete,
    SessionFinished,
}

enum ReceiverInput {
    Frame(InboundFrame),
    CarouselAckTimer,
}

struct CarouselAckState {
    debounce_deadline: Option<Instant>,
    heartbeat_deadline: Instant,
}

impl CarouselAckState {
    fn new(now: Instant, config: crate::node::session::runtime::CarouselRuntimeConfig) -> Self {
        Self {
            debounce_deadline: Some(checked_deadline(now, config.ack_debounce)),
            heartbeat_deadline: checked_deadline(now, config.ack_heartbeat),
        }
    }

    fn note_progress(
        &mut self,
        now: Instant,
        config: crate::node::session::runtime::CarouselRuntimeConfig,
    ) {
        self.debounce_deadline
            .get_or_insert(checked_deadline(now, config.ack_debounce));
    }

    fn next_deadline(&self) -> Instant {
        self.debounce_deadline
            .map_or(self.heartbeat_deadline, |debounce| {
                debounce.min(self.heartbeat_deadline)
            })
    }

    fn record_sent(
        &mut self,
        now: Instant,
        config: crate::node::session::runtime::CarouselRuntimeConfig,
    ) {
        self.debounce_deadline = None;
        self.heartbeat_deadline = checked_deadline(now, config.ack_heartbeat);
    }
}

/// Receiver state that is truly common across plain and FEC modes.
pub(super) struct ReceiverShared {
    pub(super) session_id: SessionId,
    pub(super) route: TransportRoute,
    pub(super) local_node_id: usize,
    pub(super) cfg: ReceiverConfig,
    pub(super) processors: ProcessorHandle,
    pub(super) manifest: Option<LosslessSessionManifest>,
    pub(super) plan: Option<BlockPlan>,
    pub(super) complete_blocks: BTreeSet<u64>,
    pub(super) metrics: Arc<SessionMetrics>,
}

#[derive(Debug)]
pub(super) enum SinkWriteError {
    InvalidRange(&'static str),
    BufferCapacity {
        required: usize,
    },
    BufferRange {
        start: usize,
        end: usize,
        sink_len: usize,
    },
    FileMetadata(io::Error),
    FileResize(io::Error),
    FileSeek(io::Error),
    FileWrite(io::Error),
}

impl std::fmt::Display for SinkWriteError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InvalidRange(reason) => write!(f, "invalid sink write range: {reason}"),
            Self::BufferCapacity { required } => {
                write!(f, "sink buffer could not reserve {required} bytes")
            }
            Self::BufferRange {
                start,
                end,
                sink_len,
            } => write!(
                f,
                "sink buffer range {start}..{end} exceeds buffer length {sink_len}"
            ),
            Self::FileMetadata(err) => write!(f, "sink file metadata failed: {err}"),
            Self::FileResize(err) => write!(f, "sink file resize failed: {err}"),
            Self::FileSeek(err) => write!(f, "sink file seek failed: {err}"),
            Self::FileWrite(err) => write!(f, "sink file write failed: {err}"),
        }
    }
}

impl std::error::Error for SinkWriteError {}

/// Concrete receiver mode selected after the manifest is installed.
enum ReceiverMode {
    Plain(PlainReceiver),
    Cloudcast(CloudcastReceiver),
    Fec(FecReceiver),
    MettleCarousel(Box<MettleCarouselReceiver>),
}

impl SessionReceiver {
    /// Build receiver state for one lossless session.
    #[cfg(test)]
    fn new(cfg: ReceiverConfig, processors: ProcessorHandle) -> Self {
        Self::new_with_metrics(cfg, processors, Arc::new(SessionMetrics::default()))
    }

    fn new_with_metrics(
        cfg: ReceiverConfig,
        processors: ProcessorHandle,
        metrics: Arc<SessionMetrics>,
    ) -> Self {
        Self {
            shared: ReceiverShared {
                session_id: cfg.session_id,
                route: cfg.route,
                local_node_id: cfg.local_node_id,
                cfg,
                processors,
                manifest: None,
                plan: None,
                complete_blocks: BTreeSet::new(),
                metrics,
            },
            mode: None,
            lifecycle: ReceiverLifecycle::Active,
            passive_complete_deadline: None,
            pending_control_frames: VecDeque::new(),
            carousel_ack: None,
        }
    }

    /// Execute the receiver loop until the object is complete.
    async fn run(
        &mut self,
        control_rx: &mut mpsc::Receiver<InboundFrame>,
        data_rx: &mut mpsc::Receiver<InboundFrame>,
        runtime_sender: Option<mpsc::Sender<LosslessRuntimeMessage>>,
    ) -> SessionOutcome {
        info!(
            session_id = self.shared.session_id,
            "Lossless receiver started"
        );

        loop {
            let Some(input) = self.next_input(control_rx, data_rx).await else {
                break;
            };

            match input {
                ReceiverInput::Frame(frame) => {
                    let ack_before = self.block_ack();
                    if let Err(error) = self.handle_frame(frame).await {
                        warn!(
                            session_id = self.shared.session_id,
                            error = %error,
                            "Lossless receiver aborted after sink failure"
                        );
                        self.finish_session("sink_error");
                        return SessionOutcome::SinkError;
                    }
                    if self.block_ack() != ack_before {
                        self.note_carousel_progress();
                    }
                }
                ReceiverInput::CarouselAckTimer => self.send_carousel_ack().await,
            }

            if self.lifecycle == ReceiverLifecycle::SessionFinished {
                break;
            }

            if matches!(
                self.mode.as_ref(),
                Some(ReceiverMode::MettleCarousel(mode)) if mode.aborted()
            ) {
                self.finish_session("mettle_decoder_abort");
                break;
            }

            if self.reported_complete() {
                self.enter_passive_complete();
            }
        }

        if self.reported_complete() {
            self.shared.log_payload_phase_throughput();
            self.register_completed_replay(runtime_sender, control_rx, data_rx)
                .await;
        }

        debug!(
            session_id = self.shared.session_id,
            complete = self.reported_complete(),
            lifecycle = ?self.lifecycle,
            "Lossless receiver finished"
        );

        if self.reported_complete() {
            SessionOutcome::Completed
        } else {
            SessionOutcome::Aborted
        }
    }

    async fn handle_frame(&mut self, frame: InboundFrame) -> Result<(), SinkWriteError> {
        if lossless_session::decode_control(&frame.bytes).is_some() {
            self.handle_control_frame(frame).await
        } else if lossless_session::decode_block_data(&frame.bytes).is_some() {
            self.handle_block_data_frame(frame).await
        } else if lossless_session::decode_block_symbol(&frame.bytes).is_some() {
            self.handle_block_symbol_frame(frame).await
        } else {
            Ok(())
        }
    }

    async fn next_input(
        &mut self,
        control_rx: &mut mpsc::Receiver<InboundFrame>,
        data_rx: &mut mpsc::Receiver<InboundFrame>,
    ) -> Option<ReceiverInput> {
        if self
            .next_carousel_ack_deadline()
            .is_some_and(|deadline| deadline <= Instant::now())
        {
            return Some(ReceiverInput::CarouselAckTimer);
        }

        if let Some(frame) = self.pending_control_frames.pop_front() {
            return Some(ReceiverInput::Frame(
                self.defer_source_done_behind_ready_data(frame, data_rx)
                    .await,
            ));
        }

        if let Ok(frame) = control_rx.try_recv() {
            let frame = self.coalesce_control_frame(frame, control_rx);
            return Some(ReceiverInput::Frame(
                self.defer_source_done_behind_ready_data(frame, data_rx)
                    .await,
            ));
        }

        let ack_deadline = self.next_carousel_ack_deadline();
        let maybe_frame = if self.is_passive_complete() {
            let passive_deadline = self.passive_complete_deadline();
            if passive_deadline <= Instant::now() {
                self.finish_session("session_finish_timeout");
                return None;
            }
            tokio::select! {
                biased;
                _ = sleep_until_optional(ack_deadline) => {
                    return Some(ReceiverInput::CarouselAckTimer);
                }
                maybe_frame = control_rx.recv() => maybe_frame
                    .map(|frame| self.coalesce_control_frame(frame, control_rx)),
                maybe_frame = data_rx.recv() => maybe_frame,
                _ = tokio::time::sleep_until(passive_deadline) => {
                    self.finish_session("session_finish_timeout");
                    return None;
                }
            }
        } else {
            tokio::select! {
                biased;
                _ = sleep_until_optional(ack_deadline) => {
                    return Some(ReceiverInput::CarouselAckTimer);
                }
                maybe_frame = control_rx.recv() => maybe_frame
                    .map(|frame| self.coalesce_control_frame(frame, control_rx)),
                maybe_frame = data_rx.recv() => maybe_frame,
            }
        };

        if maybe_frame.is_none() {
            self.finish_session("receiver_channel_closed");
        }
        Some(ReceiverInput::Frame(
            self.defer_source_done_behind_ready_data(maybe_frame?, data_rx)
                .await,
        ))
    }

    #[cfg(test)]
    async fn next_frame(
        &mut self,
        control_rx: &mut mpsc::Receiver<InboundFrame>,
        data_rx: &mut mpsc::Receiver<InboundFrame>,
    ) -> Option<InboundFrame> {
        loop {
            match self.next_input(control_rx, data_rx).await? {
                ReceiverInput::Frame(frame) => return Some(frame),
                ReceiverInput::CarouselAckTimer => self.send_carousel_ack().await,
            }
        }
    }

    fn coalesce_control_frame(
        &mut self,
        mut frame: InboundFrame,
        control_rx: &mut mpsc::Receiver<InboundFrame>,
    ) -> InboundFrame {
        let mut newest_round = source_done_round(&frame);
        while let Ok(next) = control_rx.try_recv() {
            match (newest_round, source_done_round(&next)) {
                (Some(current_round), Some(next_round)) => {
                    if next_round >= current_round {
                        frame = next;
                        newest_round = Some(next_round);
                    }
                }
                _ => self.pending_control_frames.push_back(next),
            }
        }
        frame
    }

    async fn defer_source_done_behind_ready_data(
        &mut self,
        frame: InboundFrame,
        data_rx: &mut mpsc::Receiver<InboundFrame>,
    ) -> InboundFrame {
        if source_done_round(&frame).is_none() {
            return frame;
        }

        if matches!(self.mode, Some(ReceiverMode::Fec(_))) {
            return frame;
        }

        if let Ok(data_frame) = data_rx.try_recv() {
            self.pending_control_frames.push_front(frame);
            return data_frame;
        }
        frame
    }

    fn reported_complete(&self) -> bool {
        if self.is_carousel() {
            return self.object_complete();
        }
        match self.mode.as_ref() {
            Some(ReceiverMode::Plain(mode)) => mode.is_complete(),
            Some(ReceiverMode::Cloudcast(mode)) => mode.is_complete(),
            Some(ReceiverMode::Fec(mode)) => mode.is_complete(),
            Some(ReceiverMode::MettleCarousel(mode)) => mode.is_complete(),
            None => false,
        }
    }

    #[cfg(test)]
    fn is_complete(&self) -> bool {
        self.reported_complete()
    }

    fn object_complete(&self) -> bool {
        if let Some(ReceiverMode::MettleCarousel(mode)) = self.mode.as_ref() {
            return mode.is_complete();
        }
        self.shared.has_all_blocks()
    }

    fn block_ack(&self) -> Option<BlockAck> {
        match self.mode.as_ref() {
            Some(ReceiverMode::MettleCarousel(mode)) => mode.block_ack(),
            _ => self.shared.block_ack(),
        }
    }

    fn is_passive_complete(&self) -> bool {
        self.lifecycle == ReceiverLifecycle::PassiveComplete
    }

    fn finish_session(&mut self, reason: &'static str) {
        self.lifecycle = ReceiverLifecycle::SessionFinished;
        self.passive_complete_deadline = None;
        debug!(
            session_id = self.shared.session_id,
            reason,
            object_complete = self.object_complete(),
            "Lossless receiver entered session-finished state"
        );
    }

    fn enter_passive_complete(&mut self) {
        if self.lifecycle == ReceiverLifecycle::PassiveComplete {
            return;
        }
        self.shared.mark_object_complete();
        self.lifecycle = ReceiverLifecycle::PassiveComplete;
        self.passive_complete_deadline = Some(self.compute_passive_complete_deadline());
        debug!(
            session_id = self.shared.session_id,
            object_complete = self.object_complete(),
            "Lossless receiver entered passive-complete state"
        );
    }

    fn passive_complete_deadline(&mut self) -> Instant {
        if let Some(deadline) = self.passive_complete_deadline {
            return deadline;
        }
        let deadline = self.compute_passive_complete_deadline();
        self.passive_complete_deadline = Some(deadline);
        deadline
    }

    fn compute_passive_complete_deadline(&self) -> Instant {
        if self.is_carousel() {
            return checked_deadline(
                Instant::now(),
                self.shared.cfg.carousel.receiver_passive_window,
            );
        }
        checked_deadline(
            Instant::now(),
            timing::session_finish_timeout_for(tokio::time::Duration::from_millis(
                self.shared.cfg.peer_report_timeout_ms,
            )),
        )
    }

    fn is_carousel(&self) -> bool {
        matches!(
            self.shared.manifest.as_ref().map(|manifest| &manifest.mode),
            Some(LosslessSessionMode::Fec(fec))
                if fec.feedback_mode == FecFeedbackMode::Carousel
        )
    }

    fn next_carousel_ack_deadline(&self) -> Option<Instant> {
        let ack_deadline = self
            .carousel_ack
            .as_ref()
            .map(CarouselAckState::next_deadline);
        let repair_deadline = match self.mode.as_ref() {
            Some(ReceiverMode::MettleCarousel(mode)) => mode.repair_deadline(),
            _ => None,
        };
        match (ack_deadline, repair_deadline) {
            (Some(lhs), Some(rhs)) => Some(lhs.min(rhs)),
            (Some(deadline), None) | (None, Some(deadline)) => Some(deadline),
            (None, None) => None,
        }
    }

    fn note_carousel_progress(&mut self) {
        if let Some(ack) = self.carousel_ack.as_mut() {
            ack.note_progress(Instant::now(), self.shared.cfg.carousel);
        }
    }

    async fn send_carousel_ack(&mut self) {
        let Some(ack) = self.block_ack() else {
            if let Some(state) = self.carousel_ack.as_mut() {
                // An armed carousel timer without an installed FEC manifest
                // must still advance; otherwise its expired deadline spins the
                // receiver loop without awaiting input.
                state.record_sent(Instant::now(), self.shared.cfg.carousel);
            }
            return;
        };
        self.shared.send_block_ack(&ack).await;
        if let Some(ReceiverMode::MettleCarousel(mode)) = self.mode.as_mut() {
            mode.note_ack_sent();
        }
        if let Some(state) = self.carousel_ack.as_mut() {
            state.record_sent(Instant::now(), self.shared.cfg.carousel);
        }
    }

    /// Handle one inbound control frame.
    async fn handle_control_frame(&mut self, frame: InboundFrame) -> Result<(), SinkWriteError> {
        let Some((_, control)) = lossless_session::decode_control(&frame.bytes) else {
            return Ok(());
        };

        match control {
            LosslessSessionControl::Manifest { manifest } => {
                info!(
                    session_id = self.shared.session_id,
                    local_node_id = self.shared.local_node_id,
                    total_bytes = manifest.total_bytes,
                    total_blocks = manifest.total_blocks,
                    fec = manifest.mode.is_fec(),
                    "Lossless receiver received manifest control frame"
                );
                self.install_manifest(manifest).await?;
            }
            LosslessSessionControl::Ready
            | LosslessSessionControl::Need { .. }
            | LosslessSessionControl::BlockAck { .. } => {}
            LosslessSessionControl::AckProbe { target_peer_id } => {
                if self.is_carousel()
                    && u64::try_from(self.shared.local_node_id).ok() == Some(target_peer_id)
                {
                    self.send_carousel_ack().await;
                }
            }
            LosslessSessionControl::SessionComplete => {
                if self.is_carousel() && self.is_passive_complete() {
                    self.finish_session("session_complete");
                }
            }
            LosslessSessionControl::DepartureCheckpoint {
                stream_id,
                repair_epoch,
                departure_bin_exclusive,
            } => {
                let Some(manifest) = self.shared.manifest.as_ref() else {
                    return Ok(());
                };
                let control = LosslessSessionControl::DepartureCheckpoint {
                    stream_id,
                    repair_epoch,
                    departure_bin_exclusive,
                };
                if manifest.validate_control(&control).is_err() {
                    return Ok(());
                }
                if let Some(ReceiverMode::MettleCarousel(mode)) = self.mode.as_mut() {
                    mode.handle_departure_checkpoint(
                        stream_id,
                        repair_epoch,
                        departure_bin_exclusive,
                        self.shared.cfg.carousel.mettle_repair_reorder_budget,
                    );
                }
            }
            LosslessSessionControl::SourceDone { round_id } => {
                if self.is_carousel() {
                    return Ok(());
                }
                if let Some(ReceiverMode::Plain(mode)) = self.mode.as_mut() {
                    mode.handle_source_done(&self.shared, round_id).await;
                }
                if let Some(ReceiverMode::Cloudcast(mode)) = self.mode.as_mut() {
                    mode.handle_source_done(&self.shared, round_id).await;
                }
                if let Some(ReceiverMode::Fec(mode)) = self.mode.as_mut() {
                    mode.handle_source_done(&self.shared, round_id).await;
                }
            }
        }
        Ok(())
    }

    /// Dispatch one plain data frame when the installed manifest is plain.
    async fn handle_block_data_frame(&mut self, frame: InboundFrame) -> Result<(), SinkWriteError> {
        match self.mode.as_mut() {
            Some(ReceiverMode::Plain(mode)) => {
                mode.handle_block_data_frame(&mut self.shared, frame).await
            }
            Some(ReceiverMode::Cloudcast(mode)) => {
                mode.handle_block_data_frame(&mut self.shared, frame).await
            }
            _ => Ok(()),
        }
    }

    /// Dispatch one FEC symbol frame when the installed manifest is FEC.
    async fn handle_block_symbol_frame(
        &mut self,
        frame: InboundFrame,
    ) -> Result<(), SinkWriteError> {
        match self.mode.as_mut() {
            Some(ReceiverMode::Fec(mode)) => {
                mode.handle_block_symbol_frame(&mut self.shared, frame)
                    .await
            }
            Some(ReceiverMode::MettleCarousel(mode)) => {
                mode.handle_block_symbol_frame(&mut self.shared, frame)
                    .await
            }
            _ => Ok(()),
        }
    }

    /// Install the first valid manifest and send READY.
    async fn install_manifest(
        &mut self,
        manifest: LosslessSessionManifest,
    ) -> Result<(), SinkWriteError> {
        if let Some(existing) = &self.shared.manifest {
            if existing == &manifest {
                self.shared.send_ready().await;
            } else {
                warn!(
                    session_id = self.shared.session_id,
                    installed_total_bytes = existing.total_bytes,
                    installed_block_size = existing.block_size,
                    received_total_bytes = manifest.total_bytes,
                    received_block_size = manifest.block_size,
                    "Lossless receiver ignored conflicting manifest after install"
                );
            }
            return Ok(());
        }

        if let Err(error) = manifest.validate() {
            warn!(
                session_id = self.shared.session_id,
                ?error,
                "Lossless receiver rejected an invalid manifest"
            );
            return Ok(());
        }
        if manifest.mode.is_fec() && !self.shared.cfg.fec_enabled {
            warn!(
                session_id = self.shared.session_id,
                "Lossless receiver rejected FEC manifest because local runtime disabled FEC"
            );
            return Ok(());
        }
        if let LosslessSessionMode::Fec(fec) = &manifest.mode
            && !receiver_supports_fec_scheme(fec)
        {
            return Ok(());
        }
        if matches!(
            &manifest.mode,
            LosslessSessionMode::Fec(fec)
                if fec.feedback_mode == FecFeedbackMode::Carousel
        ) && let Err(err) = self.shared.cfg.carousel.validate()
        {
            warn!(
                session_id = self.shared.session_id,
                %err,
                "Lossless receiver rejected carousel manifest with invalid timing"
            );
            return Ok(());
        }

        let Ok(block_size) = usize::try_from(manifest.block_size) else {
            return Ok(());
        };
        let Ok(plan) = BlockPlan::new(manifest.total_bytes, block_size) else {
            return Ok(());
        };
        let mode = match &manifest.mode {
            LosslessSessionMode::Plain => {
                if let Some(cloudcast) = self.shared.cfg.cloudcast.as_ref() {
                    ReceiverMode::Cloudcast(CloudcastReceiver::new(cloudcast.tree_ids()))
                } else {
                    ReceiverMode::Plain(PlainReceiver::default())
                }
            }
            LosslessSessionMode::Fec(fec) => {
                let Ok(validated_geometry) =
                    crate::node::session::fec::validate_fec_geometry(manifest.block_size, fec)
                else {
                    warn!(
                        session_id = self.shared.session_id,
                        "Lossless receiver rejected manifest with invalid codec geometry"
                    );
                    return Ok(());
                };
                let Some(geometry) = SymbolGeometry::from_wire(validated_geometry.wire()).ok()
                else {
                    return Ok(());
                };
                if mettle_carousel::is_mettle_carousel(&manifest.mode) {
                    let object_geometry = fec
                        .mettle_object_stream
                        .expect("validated METTLE carousel manifest has object geometry");
                    let Ok(object_plan) =
                        ObjectSymbolPlan::from_negotiated(manifest.total_bytes, object_geometry)
                    else {
                        self.finish_session("mettle_manifest_invalid_geometry");
                        return Ok(());
                    };
                    match MettleCarouselReceiver::install(
                        self.shared.session_id,
                        object_plan,
                        fec.clone(),
                        self.shared.cfg.mettle_decoder_budget.as_ref(),
                    )
                    .await
                    {
                        Ok(mode) => ReceiverMode::MettleCarousel(Box::new(mode)),
                        Err(error) => {
                            warn!(
                                session_id = self.shared.session_id,
                                %error,
                                "Lossless receiver rejected METTLE carousel before Ready"
                            );
                            self.finish_session("mettle_decoder_rejected");
                            return Ok(());
                        }
                    }
                } else {
                    ReceiverMode::Fec(FecReceiver::new(
                        geometry,
                        validated_geometry.symbol_id_bounds(),
                    ))
                }
            }
        };

        let Some(object_len) = plan.total_bytes_usize() else {
            warn!(
                session_id = self.shared.session_id,
                manifest_total_bytes = manifest.total_bytes,
                "Lossless receiver rejected manifest that did not fit local address space"
            );
            return Ok(());
        };
        self.shared.ensure_sink_len(object_len).await?;
        self.shared.plan = Some(plan);
        self.shared.manifest = Some(manifest);
        self.mode = Some(mode);
        if self.is_carousel() {
            self.carousel_ack = Some(CarouselAckState::new(
                Instant::now(),
                self.shared.cfg.carousel,
            ));
        }
        info!(
            session_id = self.shared.session_id,
            local_node_id = self.shared.local_node_id,
            total_bytes = self
                .shared
                .manifest
                .as_ref()
                .map(|manifest| manifest.total_bytes)
                .unwrap_or_default(),
            total_blocks = self
                .shared
                .manifest
                .as_ref()
                .map(|manifest| manifest.total_blocks)
                .unwrap_or_default(),
            fec = self
                .shared
                .manifest
                .as_ref()
                .is_some_and(|manifest| manifest.mode.is_fec()),
            "Lossless receiver installed manifest"
        );
        self.shared.send_ready().await;
        Ok(())
    }

    async fn register_completed_replay(
        &mut self,
        runtime_sender: Option<mpsc::Sender<LosslessRuntimeMessage>>,
        control_rx: &mut mpsc::Receiver<InboundFrame>,
        data_rx: &mut mpsc::Receiver<InboundFrame>,
    ) {
        let Some(runtime_sender) = runtime_sender else {
            return;
        };
        let Some(replay) = self.completed_replay() else {
            return;
        };

        let (ack_tx, ack_rx) = oneshot::channel();
        if runtime_sender
            .send(LosslessRuntimeMessage::ReceiverCompleted {
                session_id: self.shared.session_id,
                replay,
                ack: ack_tx,
            })
            .await
            .is_ok()
        {
            tokio::pin!(ack_rx);
            let mut control_open = true;
            let mut data_open = true;
            loop {
                tokio::select! {
                    biased;
                    _ = &mut ack_rx => break,
                    maybe_frame = control_rx.recv(), if control_open => {
                        if let Some(frame) = maybe_frame {
                            // Keep the live receiver responsive until the actor
                            // confirms that its replay is installed. In
                            // particular, an AckProbe queued ahead of
                            // ReceiverCompleted still receives the final ack.
                            let _ = self.handle_control_frame(frame).await;
                        } else {
                            control_open = false;
                        }
                    }
                    maybe_frame = data_rx.recv(), if data_open => {
                        if maybe_frame.is_none() {
                            data_open = false;
                        }
                        // Completed payload tails are intentionally discarded.
                    }
                }
            }
        }
    }

    fn completed_replay(&self) -> Option<CompletedReceiverReplay> {
        let rounds_retain_until = || {
            checked_deadline(
                Instant::now(),
                timing::session_finish_timeout_for(tokio::time::Duration::from_millis(
                    self.shared.cfg.peer_report_timeout_ms,
                )),
            )
        };
        if self.is_carousel() && self.reported_complete() {
            return Some(CompletedReceiverReplay::Carousel {
                route: self.shared.route,
                ack: self.block_ack()?,
                local_node_id: self.shared.local_node_id,
                retain_until: checked_deadline(
                    Instant::now(),
                    self.shared.cfg.carousel.receiver_passive_window,
                ),
            });
        }
        match self.mode.as_ref() {
            Some(ReceiverMode::Plain(mode)) if self.reported_complete() => {
                let round_id = mode.last_source_done_round_id()?;
                Some(CompletedReceiverReplay::Plain {
                    round_id,
                    route: self.shared.route,
                    report: NeedReport::Complete,
                    retain_until: rounds_retain_until(),
                })
            }
            Some(ReceiverMode::Cloudcast(mode)) if self.reported_complete() => {
                let round_id = mode.last_source_done_round_id()?;
                Some(CompletedReceiverReplay::Plain {
                    round_id,
                    route: self.shared.route,
                    report: NeedReport::Complete,
                    retain_until: rounds_retain_until(),
                })
            }
            Some(ReceiverMode::Fec(mode)) if self.reported_complete() => {
                let round_id = mode.last_source_done_round_id()?;
                Some(CompletedReceiverReplay::Fec {
                    round_id,
                    route: self.shared.route,
                    report: NeedReport::Complete,
                    retain_until: rounds_retain_until(),
                })
            }
            _ => None,
        }
    }
}

fn checked_deadline(now: Instant, duration: tokio::time::Duration) -> Instant {
    now.checked_add(duration).unwrap_or(now)
}

async fn sleep_until_optional(deadline: Option<Instant>) {
    if let Some(deadline) = deadline {
        tokio::time::sleep_until(deadline).await;
    } else {
        pending::<()>().await;
    }
}

fn source_done_round(frame: &InboundFrame) -> Option<u32> {
    let (_, control) = lossless_session::decode_control(&frame.bytes)?;
    match control {
        LosslessSessionControl::SourceDone { round_id } => Some(round_id),
        _ => None,
    }
}

fn receiver_supports_fec_scheme(fec: &LosslessSessionFecMode) -> bool {
    match (fec.scheme_kind(), fec.feedback_mode) {
        (Some(FecScheme::RaptorQ), _) => true,
        (Some(FecScheme::Mettle), FecFeedbackMode::Rounds) => true,
        (Some(FecScheme::Mettle), FecFeedbackMode::Carousel) => fec.mettle_object_stream.is_some(),
        (None, _) => false,
    }
}

impl ReceiverShared {
    /// Return whether the receiver has completed every planned block.
    fn has_all_blocks(&self) -> bool {
        let Some(plan) = self.plan else {
            return false;
        };
        self.complete_blocks.len() as u64 == plan.total_blocks()
    }

    fn block_ack(&self) -> Option<BlockAck> {
        let total_blocks = self.plan?.total_blocks();
        let mut completed_watermark = 0u64;
        while completed_watermark < total_blocks
            && self.complete_blocks.contains(&completed_watermark)
        {
            completed_watermark = completed_watermark.checked_add(1)?;
        }

        let mut extra_completed = Vec::new();
        let mut current_start = None;
        let mut current_end = completed_watermark;
        for &block_id in self.complete_blocks.range(completed_watermark..) {
            if block_id >= total_blocks {
                break;
            }
            if current_start.is_some() && block_id == current_end {
                current_end = block_id.checked_add(1)?;
                continue;
            }
            if let Some(start_block_id) = current_start.replace(block_id) {
                extra_completed.push(CompletedBlockRange {
                    start_block_id,
                    end_block_id: current_end,
                });
            }
            current_end = block_id.checked_add(1)?;
        }
        if let Some(start_block_id) = current_start {
            extra_completed.push(CompletedBlockRange {
                start_block_id,
                end_block_id: current_end,
            });
        }

        BlockAck::Blocks {
            completed_watermark,
            extra_completed,
        }
        .for_wire(total_blocks)
        .ok()
    }

    /// Record when the first payload unit arrives for this receiver session.
    pub(super) fn mark_first_payload_unit(&self) {
        if let Some(progress) = &self.cfg.progress {
            progress.mark_first_payload_unit();
        }
    }

    /// Record when this receiver first completed the local object.
    fn mark_object_complete(&self) {
        if let Some(progress) = &self.cfg.progress {
            progress.mark_object_complete();
        }
    }

    /// Log payload-phase receiver throughput when first-payload timing is available.
    fn log_payload_phase_throughput(&self) {
        let Some(progress) = &self.cfg.progress else {
            return;
        };
        let Some(first_payload_at) = progress.first_payload_unit_at() else {
            return;
        };
        let Some(object_complete_at) = progress.object_complete_at() else {
            return;
        };
        let Some(manifest) = &self.manifest else {
            return;
        };
        let payload_phase = object_complete_at.saturating_duration_since(first_payload_at);
        if payload_phase.is_zero() {
            return;
        }
        let receiver_mbps =
            manifest.total_bytes as f64 * 8.0 / payload_phase.as_secs_f64() / 1_000_000.0;
        info!(
            session_id = self.session_id,
            local_node_id = self.local_node_id,
            total_bytes = manifest.total_bytes,
            payload_phase_ms = payload_phase.as_millis() as u64,
            receiver_mbps,
            "Lossless receiver payload-phase throughput"
        );
    }

    /// Copy one completed block payload into the optional sink buffer.
    pub(super) async fn write_block(
        &self,
        block_id: u64,
        payload: &[u8],
    ) -> Result<(), SinkWriteError> {
        let plan = self
            .plan
            .ok_or(SinkWriteError::InvalidRange("missing block plan"))?;
        let span = plan
            .block_span(block_id)
            .ok_or(SinkWriteError::InvalidRange("block id is outside the plan"))?;
        let copy_len = payload.len().min(span.len());
        if copy_len == 0 {
            return Ok(());
        }

        let start = usize::try_from(span.offset())
            .map_err(|_| SinkWriteError::InvalidRange("block offset does not fit host"))?;
        let end = start
            .checked_add(copy_len)
            .ok_or(SinkWriteError::InvalidRange("block sink range overflow"))?;

        if let Some(sink) = &self.cfg.sink_buffer {
            let mut guard = sink.lock().await;
            if end > guard.len() {
                return Err(SinkWriteError::BufferRange {
                    start,
                    end,
                    sink_len: guard.len(),
                });
            }
            guard[start..end].copy_from_slice(&payload[..copy_len]);
        }

        if let Some(sink) = &self.cfg.sink_file {
            let mut guard = sink.lock().await;
            guard
                .seek(SeekFrom::Start(span.offset()))
                .map_err(SinkWriteError::FileSeek)?;
            guard
                .write_all(&payload[..copy_len])
                .map_err(SinkWriteError::FileWrite)?;
        }

        Ok(())
    }

    /// Copy one contiguous run of decoded source symbols into the optional sinks.
    pub(super) async fn write_symbol_run(
        &self,
        block_id: u64,
        geometry: SymbolGeometry,
        first_source_index: usize,
        payload: &[u8],
    ) -> Result<(), SinkWriteError> {
        let plan = self
            .plan
            .ok_or(SinkWriteError::InvalidRange("missing block plan"))?;
        let span = plan
            .block_span(block_id)
            .ok_or(SinkWriteError::InvalidRange("block id is outside the plan"))?;

        let symbol_offset = first_source_index
            .checked_mul(geometry.symbol_size())
            .ok_or(SinkWriteError::InvalidRange(
                "source-symbol offset overflow",
            ))?;
        if symbol_offset >= span.len() {
            return Err(SinkWriteError::InvalidRange(
                "source-symbol offset is outside the block",
            ));
        }
        let copy_len = payload.len().min(span.len() - symbol_offset);
        if copy_len == 0 {
            return Ok(());
        }
        let start = usize::try_from(span.offset())
            .ok()
            .and_then(|offset| offset.checked_add(symbol_offset))
            .ok_or(SinkWriteError::InvalidRange("symbol sink offset overflow"))?;
        let end = start
            .checked_add(copy_len)
            .ok_or(SinkWriteError::InvalidRange("symbol sink range overflow"))?;

        if let Some(sink) = &self.cfg.sink_buffer {
            let mut guard = sink.lock().await;
            if end > guard.len() {
                return Err(SinkWriteError::BufferRange {
                    start,
                    end,
                    sink_len: guard.len(),
                });
            }
            guard[start..end].copy_from_slice(&payload[..copy_len]);
        }

        if let Some(sink) = &self.cfg.sink_file {
            let file_offset = u64::try_from(symbol_offset)
                .ok()
                .and_then(|offset| span.offset().checked_add(offset))
                .ok_or(SinkWriteError::InvalidRange("symbol file offset overflow"))?;
            let mut guard = sink.lock().await;
            guard
                .seek(SeekFrom::Start(file_offset))
                .map_err(SinkWriteError::FileSeek)?;
            guard
                .write_all(&payload[..copy_len])
                .map_err(SinkWriteError::FileWrite)?;
        }

        Ok(())
    }

    /// Commit one decoded object-stream source at its global object offset.
    /// The write completes before the METTLE watermark is advanced.
    pub(super) async fn write_object_symbol(
        &self,
        plan: ObjectSymbolPlan,
        global_source_id: u64,
        payload: &[u8],
    ) -> Result<(), SinkWriteError> {
        let span = plan
            .source_span(global_source_id)
            .ok_or(SinkWriteError::InvalidRange(
                "global source id is outside the object plan",
            ))?;
        let copy_len = payload.len().min(span.len());
        if copy_len == 0 {
            return Ok(());
        }
        let start = usize::try_from(span.offset())
            .map_err(|_| SinkWriteError::InvalidRange("object source offset does not fit host"))?;
        let end = start
            .checked_add(copy_len)
            .ok_or(SinkWriteError::InvalidRange("object source range overflow"))?;

        if let Some(sink) = &self.cfg.sink_buffer {
            let mut guard = sink.lock().await;
            if end > guard.len() {
                return Err(SinkWriteError::BufferRange {
                    start,
                    end,
                    sink_len: guard.len(),
                });
            }
            guard[start..end].copy_from_slice(&payload[..copy_len]);
        }

        if let Some(sink) = &self.cfg.sink_file {
            let mut guard = sink.lock().await;
            guard
                .seek(SeekFrom::Start(span.offset()))
                .map_err(SinkWriteError::FileSeek)?;
            guard
                .write_all(&payload[..copy_len])
                .map_err(SinkWriteError::FileWrite)?;
        }
        Ok(())
    }

    /// Ensure optional sinks are large enough for the full object.
    async fn ensure_sink_len(&self, object_len: usize) -> Result<(), SinkWriteError> {
        if let Some(sink) = &self.cfg.sink_buffer {
            let mut guard = sink.lock().await;
            if guard.len() < object_len {
                let additional = object_len - guard.len();
                guard.try_reserve_exact(additional).map_err(|_| {
                    SinkWriteError::BufferCapacity {
                        required: object_len,
                    }
                })?;
                guard.resize(object_len, 0);
            }
        }

        if let Some(sink) = &self.cfg.sink_file {
            let guard = sink.lock().await;
            let object_len = u64::try_from(object_len)
                .map_err(|_| SinkWriteError::InvalidRange("object length does not fit file"))?;
            let current_len = guard
                .metadata()
                .map_err(SinkWriteError::FileMetadata)?
                .len();
            if current_len != object_len {
                guard
                    .set_len(object_len)
                    .map_err(SinkWriteError::FileResize)?;
            }
        }

        Ok(())
    }

    /// Send a READY control frame back to the sender.
    async fn send_ready(&self) {
        control::send_control(
            &self.processors,
            control::FrameRoute {
                session_id: self.session_id,
                tree_id: None,
                src_ip: self.route.src_ip,
                src_port: self.route.src_port,
                dst_ip: self.route.dst_ip,
                dst_port: self.route.dst_port,
            },
            &LosslessSessionControl::Ready,
        )
        .await;
    }

    async fn send_fec_need(&self, round_id: u32, report: &NeedReport) {
        control::send_control(
            &self.processors,
            control::FrameRoute {
                session_id: self.session_id,
                tree_id: None,
                src_ip: self.route.src_ip,
                src_port: self.route.src_port,
                dst_ip: self.route.dst_ip,
                dst_port: self.route.dst_port,
            },
            &LosslessSessionControl::Need {
                round_id,
                report: report.clone(),
            },
        )
        .await;
    }

    async fn send_block_ack(&self, ack: &BlockAck) {
        control::send_control(
            &self.processors,
            control::FrameRoute {
                session_id: self.session_id,
                tree_id: None,
                src_ip: self.route.src_ip,
                src_port: self.route.src_port,
                dst_ip: self.route.dst_ip,
                dst_port: self.route.dst_port,
            },
            &LosslessSessionControl::BlockAck { ack: ack.clone() },
        )
        .await;
    }

    fn plain_need(&self) -> Option<NeedReport> {
        let total_blocks = self.plan?.total_blocks();
        if total_blocks == 0 {
            return Some(NeedReport::Complete);
        }
        if self.complete_blocks.len() as u64 == total_blocks {
            return Some(NeedReport::Complete);
        }

        let mut ranges = Vec::new();
        let mut next_missing = 0u64;
        while next_missing < total_blocks {
            if self.complete_blocks.contains(&next_missing) {
                next_missing += 1;
                continue;
            }

            let start_block_id = next_missing;
            let end_block_id = self
                .complete_blocks
                .range((Excluded(start_block_id), Unbounded))
                .next()
                .copied()
                .unwrap_or(total_blocks);
            ranges.push(MissingBlockRange {
                start_block_id,
                end_block_id,
            });
            next_missing = end_block_id;
        }

        Some(NeedReport::Plain { ranges })
    }

    async fn send_plain_need(&self, round_id: u32, report: &NeedReport) {
        control::send_control(
            &self.processors,
            control::FrameRoute {
                session_id: self.session_id,
                tree_id: None,
                src_ip: self.route.src_ip,
                src_port: self.route.src_port,
                dst_ip: self.route.dst_ip,
                dst_port: self.route.dst_port,
            },
            &LosslessSessionControl::Need {
                round_id,
                report: report.clone(),
            },
        )
        .await;
    }
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, BTreeSet};
    use std::net::Ipv4Addr;
    use std::sync::Arc;
    use std::time::Duration;

    use tokio::sync::mpsc;
    use tokio::time::timeout;

    use nextmini_messages::lossless_session::NeedBlock;
    use nextmini_messages::{RouteForwardingMode, RoutingTableEntry};

    use super::*;
    use crate::node::NodeIdExt;
    use crate::node::config::LocalConfig;
    use crate::node::packet::Packet;
    use crate::node::processor::ProcessorHandle;
    use crate::node::session::receiver::fec::{FecBlockState, FecReceiver};

    const SOURCE_NODE_ID: usize = 51;
    const RECEIVER_NODE_ID: usize = 52;

    #[tokio::test]
    async fn fec_receiver_replays_cached_need_after_late_symbols_for_same_round() {
        let (mut receiver, mut packet_rx) = fec_test_receiver(
            8,
            BTreeSet::new(),
            BTreeMap::from([(0, BTreeMap::from([(0, vec![1, 2])]))]),
        )
        .await;

        let source_done = InboundFrame {
            bytes: lossless_session::encode_control(
                receiver.shared.session_id,
                &LosslessSessionControl::SourceDone { round_id: 0 },
            ),
            peer_id: Some(SOURCE_NODE_ID),
        };
        let expected = NeedReport::Fec {
            blocks: vec![NeedBlock {
                block_id: 0,
                deficit_symbols: 3,
            }],
        };

        receiver
            .handle_control_frame(source_done.clone())
            .await
            .expect("test receiver sink should accept frame");
        assert_eq!(recv_fec_need(&mut packet_rx).await, expected.clone());
        assert!(!receiver.is_complete());

        receiver
            .handle_block_symbol_frame(InboundFrame {
                bytes: lossless_session::encode_block_symbol(
                    receiver.shared.session_id,
                    0,
                    1,
                    0,
                    &[3, 4],
                ),
                peer_id: Some(SOURCE_NODE_ID),
            })
            .await
            .expect("test receiver sink should accept frame");

        receiver
            .handle_control_frame(source_done)
            .await
            .expect("test receiver sink should accept frame");
        assert_eq!(recv_fec_need(&mut packet_rx).await, expected);
        assert!(!receiver.is_complete());
    }

    #[tokio::test]
    async fn block_deficit_requests_missing_source_symbols_first() {
        let shared = ReceiverShared {
            session_id: 7,
            route: crate::node::session::runtime::TransportRoute {
                src_ip: std::net::Ipv4Addr::new(10, 0, 0, 1),
                dst_ip: std::net::Ipv4Addr::new(10, 0, 0, 2),
                src_port: 1,
                dst_port: 2,
            },
            local_node_id: 1,
            cfg: ReceiverConfig {
                session_id: 7,
                route: crate::node::session::runtime::TransportRoute {
                    src_ip: std::net::Ipv4Addr::new(10, 0, 0, 1),
                    dst_ip: std::net::Ipv4Addr::new(10, 0, 0, 2),
                    src_port: 1,
                    dst_port: 2,
                },
                local_node_id: 1,
                sink_buffer: None,
                sink_file: None,
                progress: None,
                peer_report_timeout_ms: 200,
                fec_enabled: true,
                cloudcast: None,
                carousel: Default::default(),
                mettle_decoder_budget: None,
            },
            processors: crate::node::processor::ProcessorHandle::new(Default::default()),
            manifest: Some(LosslessSessionManifest {
                block_size: 8,
                total_bytes: 16,
                total_blocks: 2,
                mode: LosslessSessionMode::Fec(
                    nextmini_messages::lossless_session::LosslessSessionFecMode::new_raptorq(
                        4,
                        vec![0, 1],
                    ),
                ),
            }),
            plan: BlockPlan::new(16, 8).ok(),
            complete_blocks: BTreeSet::new(),
            metrics: Arc::new(SessionMetrics::default()),
        };
        let mut receiver = FecReceiver::new(
            BlockPlan::new(16, 8)
                .ok()
                .and_then(|plan| plan.symbol_geometry(4).ok())
                .expect("valid geometry"),
            crate::node::session::fec::FecSymbolIdBounds::raptorq(),
        );
        receiver.blocks = BTreeMap::from([(
            0,
            FecBlockState {
                symbols: BTreeMap::from([(0, vec![1, 2])]),
                ..Default::default()
            },
        )]);
        receiver.last_source_done_round_id = Some(0);
        receiver.last_round_need = Some(NeedReport::Fec {
            blocks: vec![NeedBlock {
                block_id: 0,
                deficit_symbols: 3,
            }],
        });

        assert_eq!(receiver.block_deficit(&shared, 0), 3);
    }

    #[tokio::test]
    async fn carousel_receiver_debounces_progress_then_sends_heartbeats() {
        let (mut receiver, mut packet_rx) = carousel_test_receiver(BTreeSet::new()).await;
        let (control_tx, mut control_rx) = mpsc::channel(1);
        let (data_tx, mut data_rx) = mpsc::channel(1);

        receiver.shared.complete_blocks.insert(0);
        receiver.note_carousel_progress();

        assert!(
            timeout(
                Duration::from_millis(5),
                receiver.next_input(&mut control_rx, &mut data_rx)
            )
            .await
            .is_err(),
            "ack must not precede the debounce deadline"
        );
        assert!(matches!(
            timeout(
                Duration::from_millis(200),
                receiver.next_input(&mut control_rx, &mut data_rx)
            )
            .await
            .expect("debounce timer should fire"),
            Some(ReceiverInput::CarouselAckTimer)
        ));
        receiver.send_carousel_ack().await;
        assert_eq!(
            recv_block_ack(&mut packet_rx).await,
            BlockAck::Blocks {
                completed_watermark: 1,
                extra_completed: vec![],
            }
        );

        assert!(matches!(
            timeout(
                Duration::from_millis(200),
                receiver.next_input(&mut control_rx, &mut data_rx)
            )
            .await
            .expect("heartbeat timer should fire"),
            Some(ReceiverInput::CarouselAckTimer)
        ));
        receiver.send_carousel_ack().await;
        assert_eq!(
            recv_block_ack(&mut packet_rx).await,
            BlockAck::Blocks {
                completed_watermark: 1,
                extra_completed: vec![],
            }
        );

        drop((control_tx, data_tx));
    }

    #[tokio::test]
    async fn carousel_receiver_answers_probes_and_finishes_only_from_passive_state() {
        let (mut receiver, mut packet_rx) = carousel_test_receiver(BTreeSet::new()).await;
        receiver
            .handle_control_frame(InboundFrame {
                bytes: lossless_session::encode_control(
                    receiver.shared.session_id,
                    &LosslessSessionControl::AckProbe {
                        target_peer_id: u64::try_from(RECEIVER_NODE_ID + 1)
                            .expect("test receiver id fits u64"),
                    },
                ),
                peer_id: Some(SOURCE_NODE_ID),
            })
            .await
            .expect("non-target probe should be harmless");
        assert!(
            packet_rx.try_recv().is_err(),
            "a receiver must not answer another peer's targeted probe"
        );
        let probe = InboundFrame {
            bytes: lossless_session::encode_control(
                receiver.shared.session_id,
                &LosslessSessionControl::AckProbe {
                    target_peer_id: u64::try_from(RECEIVER_NODE_ID)
                        .expect("test receiver id fits u64"),
                },
            ),
            peer_id: Some(SOURCE_NODE_ID),
        };
        let complete = InboundFrame {
            bytes: lossless_session::encode_control(
                receiver.shared.session_id,
                &LosslessSessionControl::SessionComplete,
            ),
            peer_id: Some(SOURCE_NODE_ID),
        };

        receiver
            .handle_control_frame(probe.clone())
            .await
            .expect("probe should not touch sinks");
        assert_eq!(
            recv_block_ack(&mut packet_rx).await,
            BlockAck::Blocks {
                completed_watermark: 0,
                extra_completed: vec![],
            }
        );
        receiver
            .handle_control_frame(complete.clone())
            .await
            .expect("early completion should be ignored");
        assert_eq!(receiver.lifecycle, ReceiverLifecycle::Active);

        receiver.shared.complete_blocks.insert(0);
        receiver.enter_passive_complete();
        receiver
            .handle_control_frame(probe)
            .await
            .expect("passive probe should not touch sinks");
        assert_eq!(
            recv_block_ack(&mut packet_rx).await,
            BlockAck::Blocks {
                completed_watermark: 1,
                extra_completed: vec![],
            }
        );
        receiver
            .handle_control_frame(complete)
            .await
            .expect("completion should not touch sinks");
        assert_eq!(receiver.lifecycle, ReceiverLifecycle::SessionFinished);
    }

    #[tokio::test]
    async fn carousel_receiver_eagerly_decodes_acks_and_commits_before_completion() {
        let (mut receiver, mut packet_rx) = carousel_test_receiver(BTreeSet::new()).await;
        let session_id = receiver.shared.session_id;
        let sink = Arc::new(tokio::sync::Mutex::new(vec![0; 8]));
        receiver.shared.cfg.sink_buffer = Some(sink.clone());
        let (control_tx, mut control_rx) = mpsc::channel(8);
        let (data_tx, mut data_rx) = mpsc::channel(8);

        let task =
            tokio::spawn(async move { receiver.run(&mut control_rx, &mut data_rx, None).await });
        for (symbol_id, payload) in [(0, [0, 10]), (1, [1, 11]), (2, [2, 12]), (3, [3, 13])] {
            data_tx
                .send(InboundFrame {
                    bytes: lossless_session::encode_block_symbol(
                        session_id, 0, symbol_id, 0, &payload,
                    ),
                    peer_id: Some(SOURCE_NODE_ID),
                })
                .await
                .expect("receiver data inbox should stay open");
        }

        loop {
            if recv_block_ack(&mut packet_rx).await
                == (BlockAck::Blocks {
                    completed_watermark: 1,
                    extra_completed: vec![],
                })
            {
                break;
            }
        }
        assert_eq!(*sink.lock().await, vec![0, 10, 1, 11, 2, 12, 3, 13]);

        control_tx
            .send(InboundFrame {
                bytes: lossless_session::encode_control(
                    session_id,
                    &LosslessSessionControl::SessionComplete,
                ),
                peer_id: Some(SOURCE_NODE_ID),
            })
            .await
            .expect("receiver control inbox should stay open");
        assert_eq!(
            timeout(Duration::from_secs(2), task)
                .await
                .expect("receiver should finish after SessionComplete")
                .expect("receiver task should not panic"),
            SessionOutcome::Completed
        );
    }

    #[tokio::test]
    async fn fec_status_reports_all_missing_blocks_for_large_transfers() {
        let plan = BlockPlan::new(300, 1).expect("plan");
        let geometry = plan.symbol_geometry(4).expect("geometry");
        let shared = ReceiverShared {
            session_id: 9,
            route: crate::node::session::runtime::TransportRoute {
                src_ip: std::net::Ipv4Addr::new(10, 0, 0, 1),
                dst_ip: std::net::Ipv4Addr::new(10, 0, 0, 2),
                src_port: 1,
                dst_port: 2,
            },
            local_node_id: 1,
            cfg: ReceiverConfig {
                session_id: 9,
                route: crate::node::session::runtime::TransportRoute {
                    src_ip: std::net::Ipv4Addr::new(10, 0, 0, 1),
                    dst_ip: std::net::Ipv4Addr::new(10, 0, 0, 2),
                    src_port: 1,
                    dst_port: 2,
                },
                local_node_id: 1,
                sink_buffer: None,
                sink_file: None,
                progress: None,
                peer_report_timeout_ms: 200,
                fec_enabled: true,
                cloudcast: None,
                carousel: Default::default(),
                mettle_decoder_budget: None,
            },
            processors: crate::node::processor::ProcessorHandle::new(Default::default()),
            manifest: Some(LosslessSessionManifest {
                block_size: 1,
                total_bytes: 300,
                total_blocks: 300,
                mode: LosslessSessionMode::Fec(
                    nextmini_messages::lossless_session::LosslessSessionFecMode::new_raptorq(
                        4,
                        vec![0, 1],
                    ),
                ),
            }),
            plan: Some(plan),
            complete_blocks: BTreeSet::new(),
            metrics: Arc::new(SessionMetrics::default()),
        };
        let receiver = FecReceiver::new(
            geometry,
            crate::node::session::fec::FecSymbolIdBounds::raptorq(),
        );

        let NeedReport::Fec { blocks } = receiver.need_report(&shared).expect("status") else {
            panic!("expected missing-block status");
        };

        assert_eq!(blocks.len(), 300);
        assert_eq!(blocks.first().map(|b| b.block_id), Some(0));
        assert_eq!(blocks.last().map(|b| b.block_id), Some(299));
    }

    #[tokio::test]
    async fn plain_receiver_only_completes_after_reporting_complete_on_source_done() {
        let receiver = SessionReceiver {
            shared: ReceiverShared {
                session_id: 8,
                route: crate::node::session::runtime::TransportRoute {
                    src_ip: std::net::Ipv4Addr::new(10, 0, 0, 1),
                    dst_ip: std::net::Ipv4Addr::new(10, 0, 0, 2),
                    src_port: 1,
                    dst_port: 2,
                },
                local_node_id: 1,
                cfg: ReceiverConfig {
                    session_id: 8,
                    route: crate::node::session::runtime::TransportRoute {
                        src_ip: std::net::Ipv4Addr::new(10, 0, 0, 1),
                        dst_ip: std::net::Ipv4Addr::new(10, 0, 0, 2),
                        src_port: 1,
                        dst_port: 2,
                    },
                    local_node_id: 1,
                    sink_buffer: None,
                    sink_file: None,
                    progress: None,
                    peer_report_timeout_ms: 200,
                    fec_enabled: false,
                    cloudcast: None,
                    carousel: Default::default(),
                    mettle_decoder_budget: None,
                },
                processors: crate::node::processor::ProcessorHandle::new(Default::default()),
                manifest: Some(LosslessSessionManifest {
                    block_size: 8,
                    total_bytes: 16,
                    total_blocks: 2,
                    mode: LosslessSessionMode::Plain,
                }),
                plan: BlockPlan::new(16, 8).ok(),
                complete_blocks: BTreeSet::from([0, 1]),
                metrics: Arc::new(SessionMetrics::default()),
            },
            mode: Some(ReceiverMode::Plain(PlainReceiver::default())),
            lifecycle: ReceiverLifecycle::Active,
            passive_complete_deadline: None,
            pending_control_frames: VecDeque::new(),
            carousel_ack: None,
        };

        assert!(!receiver.is_complete());
    }

    #[test]
    fn receiver_supports_mettle_for_small_experimental_geometry() {
        const PAPER_SCALE_METTLE_K: u32 = 2400;

        assert!(receiver_supports_fec_scheme(
            &nextmini_messages::lossless_session::LosslessSessionFecMode::new_mettle(
                PAPER_SCALE_METTLE_K,
                vec![0],
            )
        ));
        assert!(receiver_supports_fec_scheme(
            &nextmini_messages::lossless_session::LosslessSessionFecMode::new_mettle(16, vec![0])
        ));
        assert!(!receiver_supports_fec_scheme(
            &nextmini_messages::lossless_session::LosslessSessionFecMode::new_mettle(16, vec![0])
                .with_feedback_mode(FecFeedbackMode::Carousel)
        ));
        let geometry = ObjectSymbolPlan::derive(32, 2)
            .expect("valid object plan")
            .geometry();
        assert!(receiver_supports_fec_scheme(
            &nextmini_messages::lossless_session::LosslessSessionFecMode::new_mettle(4, vec![0])
                .with_feedback_mode(FecFeedbackMode::Carousel)
                .with_mettle_object_stream(geometry)
        ));
        assert!(receiver_supports_fec_scheme(
            &nextmini_messages::lossless_session::LosslessSessionFecMode::new_raptorq(4, vec![0])
        ));
    }

    #[tokio::test]
    async fn mettle_carousel_writes_global_sources_across_prefix_boundaries() {
        use std::num::NonZeroUsize;

        use nextmini_messages::lossless_session::MettleObjectStreamGeometry;

        let session_id = 0xA55A;
        let source = b"abcdefghi";
        let geometry = MettleObjectStreamGeometry::new(2, 2, 3, 1);
        let plan = ObjectSymbolPlan::from_negotiated(source.len() as u64, geometry)
            .expect("valid multi-prefix plan");
        let fec_mode =
            nextmini_messages::lossless_session::LosslessSessionFecMode::new_mettle(4, vec![0])
                .with_feedback_mode(FecFeedbackMode::Carousel)
                .with_mettle_object_stream(geometry);
        let manifest = LosslessSessionManifest {
            block_size: 8,
            total_bytes: source.len() as u64,
            total_blocks: 2,
            mode: LosslessSessionMode::Fec(fec_mode.clone()),
        };
        manifest.validate().expect("valid carousel manifest");

        let route = crate::node::session::runtime::TransportRoute {
            src_ip: Ipv4Addr::new(10, 0, 0, 2),
            dst_ip: Ipv4Addr::new(10, 0, 0, 1),
            src_port: 4752,
            dst_port: 5752,
        };
        let sink = Arc::new(tokio::sync::Mutex::new(vec![0; source.len()]));
        let budget = crate::node::session::runtime::MettleDecoderBudget::from_lossless(
            &crate::node::config::LosslessConfig::default(),
        )
        .expect("default decoder budget");
        let mode =
            MettleCarouselReceiver::install(session_id, plan, fec_mode.clone(), Some(&budget))
                .await
                .expect("dense decoder admission succeeds");
        let mut receiver = SessionReceiver {
            shared: ReceiverShared {
                session_id,
                route,
                local_node_id: RECEIVER_NODE_ID,
                cfg: ReceiverConfig {
                    session_id,
                    route,
                    local_node_id: RECEIVER_NODE_ID,
                    sink_buffer: Some(sink.clone()),
                    sink_file: None,
                    progress: None,
                    peer_report_timeout_ms: 200,
                    fec_enabled: true,
                    cloudcast: None,
                    carousel: Default::default(),
                    mettle_decoder_budget: Some(budget),
                },
                processors: ProcessorHandle::new(Default::default()),
                manifest: Some(manifest),
                plan: BlockPlan::new(source.len() as u64, 8).ok(),
                complete_blocks: BTreeSet::new(),
                metrics: Arc::new(SessionMetrics::default()),
            },
            mode: Some(ReceiverMode::MettleCarousel(Box::new(mode))),
            lifecycle: ReceiverLifecycle::Active,
            passive_complete_deadline: None,
            pending_control_frames: VecDeque::new(),
            carousel_ack: Some(CarouselAckState::new(Instant::now(), Default::default())),
        };

        for stream_id in 0..plan.stream_count() {
            let source_count = plan
                .stream_source_count(stream_id)
                .expect("stream belongs to plan");
            let mut encoder = mettle::stream::Encoder::new_terminated(
                mettle::MettleParams::new(
                    crate::node::session::fec::mettle_overhead_from_fec_mode(&fec_mode)
                        .expect("valid METTLE rate"),
                ),
                NonZeroUsize::new(plan.symbol_size()).expect("non-zero symbol size"),
                crate::node::session::fec::block_seed(session_id, stream_id),
                u64::from(source_count),
            );
            let mut bins = Vec::new();
            for local_source_id in 0..source_count {
                let global_source_id = plan
                    .global_source_id(stream_id, local_source_id)
                    .expect("global source mapping");
                let span = plan
                    .source_span(global_source_id)
                    .expect("source span mapping");
                let mut payload = vec![0; plan.symbol_size()];
                let start = usize::try_from(span.offset()).expect("test offset fits");
                payload[..span.len()].copy_from_slice(&source[start..start + span.len()]);
                bins.extend(encoder.push_source(&payload));
            }
            bins.extend(encoder.finish());
            bins.sort_by_key(|bin| bin.bin_id());
            for bin in bins {
                let (bin_id, payload) = bin.into_parts();
                receiver
                    .handle_block_symbol_frame(InboundFrame {
                        bytes: lossless_session::encode_block_symbol(
                            session_id,
                            stream_id,
                            u32::try_from(bin_id).expect("test bin id fits wire"),
                            0,
                            &payload,
                        ),
                        peer_id: Some(SOURCE_NODE_ID),
                    })
                    .await
                    .expect("decoded source commits to sink");
            }
        }

        assert!(receiver.reported_complete());
        assert_eq!(*sink.lock().await, source);
        assert_eq!(
            receiver.block_ack(),
            Some(BlockAck::MettleStream {
                stream_id: 2,
                decoded_source_watermark: 1,
                stalled: None,
            })
        );
    }

    #[tokio::test]
    async fn mettle_receiver_streams_without_retaining_session_symbol_payloads() {
        let k = 131_072u32;
        let block_size = 1_073_741_824u32;
        let plan = BlockPlan::new(u64::from(block_size), block_size as usize).expect("valid plan");
        let geometry = plan.symbol_geometry(k).expect("valid geometry");
        let route = crate::node::session::runtime::TransportRoute {
            src_ip: Ipv4Addr::new(10, 0, 0, 2),
            dst_ip: Ipv4Addr::new(10, 0, 0, 1),
            src_port: 4752,
            dst_port: 5752,
        };
        let mut shared = ReceiverShared {
            session_id: 33,
            route,
            local_node_id: RECEIVER_NODE_ID,
            cfg: ReceiverConfig {
                session_id: 33,
                route,
                local_node_id: RECEIVER_NODE_ID,
                sink_buffer: None,
                sink_file: None,
                progress: None,
                peer_report_timeout_ms: 200,
                fec_enabled: true,
                cloudcast: None,
                carousel: Default::default(),
                mettle_decoder_budget: None,
            },
            processors: ProcessorHandle::new(Default::default()),
            manifest: Some(LosslessSessionManifest {
                block_size,
                total_bytes: u64::from(block_size),
                total_blocks: 1,
                mode: LosslessSessionMode::Fec(
                    nextmini_messages::lossless_session::LosslessSessionFecMode::new_mettle(
                        k,
                        vec![0],
                    ),
                ),
            }),
            plan: Some(plan),
            complete_blocks: BTreeSet::new(),
            metrics: Arc::new(SessionMetrics::default()),
        };
        let symbol_id_bounds =
            test_fec_symbol_id_bounds(shared.manifest.as_ref().expect("METTLE manifest"));
        let mut receiver = FecReceiver::new(geometry, symbol_id_bounds);
        let payload = vec![0; geometry.symbol_size()];
        let frame = InboundFrame {
            bytes: lossless_session::encode_block_symbol(shared.session_id, 0, 0, 0, &payload),
            peer_id: Some(SOURCE_NODE_ID),
        };

        receiver
            .handle_block_symbol_frame(&mut shared, frame)
            .await
            .expect("test receiver sink should accept frame");

        let state = receiver.blocks.get(&0).expect("METTLE block state");
        assert!(
            state.symbols.is_empty(),
            "METTLE should stream bins into the decoder without retaining session payload entries"
        );
        assert!(state.mettle.is_some());
        assert_eq!(
            receiver.block_deficit(&shared, 0),
            1,
            "METTLE's finite stream reports incomplete as a completion probe, not a session repair budget"
        );
    }

    #[tokio::test]
    async fn mettle_receiver_reports_complete_as_soon_as_decoder_finishes() {
        let (mut receiver, mut packet_rx) =
            fec_test_receiver(8, BTreeSet::new(), BTreeMap::new()).await;
        let geometry = receiver
            .shared
            .plan
            .expect("test receiver has a plan")
            .symbol_geometry(4)
            .expect("valid geometry");
        receiver.shared.manifest = Some(LosslessSessionManifest {
            block_size: 8,
            total_bytes: 8,
            total_blocks: 1,
            mode: LosslessSessionMode::Fec(LosslessSessionFecMode::new_mettle(4, vec![0, 1])),
        });
        let symbol_id_bounds =
            test_fec_symbol_id_bounds(receiver.shared.manifest.as_ref().expect("METTLE manifest"));
        receiver.mode = Some(ReceiverMode::Fec(FecReceiver::new(
            geometry,
            symbol_id_bounds,
        )));

        let source_symbol_bytes =
            std::num::NonZeroUsize::new(geometry.symbol_size()).expect("non-zero symbol size");
        let mut encoder = mettle::stream::Encoder::new_terminated(
            mettle::MettleParams::new(mettle::OverheadRatio::ZERO),
            source_symbol_bytes,
            crate::node::session::fec::block_seed(receiver.shared.session_id, 0),
            4,
        );
        let mut bins = Vec::new();
        for source_id in 0..4u8 {
            let source = [source_id, source_id + 1];
            bins.extend(encoder.push_source(&source));
        }
        bins.extend(encoder.finish());

        for bin in bins {
            let (bin_id, payload) = bin.into_parts();
            receiver
                .handle_block_symbol_frame(InboundFrame {
                    bytes: lossless_session::encode_block_symbol(
                        receiver.shared.session_id,
                        0,
                        u32::try_from(bin_id).expect("small test bin id"),
                        0,
                        &payload,
                    ),
                    peer_id: Some(SOURCE_NODE_ID),
                })
                .await
                .expect("test receiver sink should accept frame");
            if receiver.is_complete() {
                break;
            }
        }

        assert!(receiver.shared.has_all_blocks());
        assert_eq!(recv_fec_need(&mut packet_rx).await, NeedReport::Complete);
        assert!(receiver.is_complete());
    }

    #[tokio::test]
    async fn fec_receiver_processes_source_done_without_waiting_for_late_data() {
        let (mut receiver, _packet_rx) =
            fec_test_receiver(8, BTreeSet::new(), BTreeMap::new()).await;
        let geometry = receiver
            .shared
            .plan
            .expect("test receiver has a plan")
            .symbol_geometry(4)
            .expect("valid geometry");
        receiver.shared.manifest = Some(LosslessSessionManifest {
            block_size: 8,
            total_bytes: 8,
            total_blocks: 1,
            mode: LosslessSessionMode::Fec(LosslessSessionFecMode::new_mettle(4, vec![0, 1])),
        });
        let symbol_id_bounds =
            test_fec_symbol_id_bounds(receiver.shared.manifest.as_ref().expect("METTLE manifest"));
        receiver.mode = Some(ReceiverMode::Fec(FecReceiver::new(
            geometry,
            symbol_id_bounds,
        )));

        let (control_tx, mut control_rx) = mpsc::channel(8);
        let (data_tx, mut data_rx) = mpsc::channel(8);
        let session_id = receiver.shared.session_id;
        control_tx
            .send(InboundFrame {
                bytes: lossless_session::encode_control(
                    session_id,
                    &LosslessSessionControl::SourceDone { round_id: 0 },
                ),
                peer_id: Some(SOURCE_NODE_ID),
            })
            .await
            .expect("SourceDone should reach receiver");

        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(20)).await;
            let payload = vec![7; geometry.symbol_size()];
            data_tx
                .send(InboundFrame {
                    bytes: lossless_session::encode_block_symbol(session_id, 0, 0, 0, &payload),
                    peer_id: Some(SOURCE_NODE_ID),
                })
                .await
                .expect("late data should reach receiver");
        });

        let first = timeout(
            Duration::from_secs(1),
            receiver.next_frame(&mut control_rx, &mut data_rx),
        )
        .await
        .expect("timed out waiting for SourceDone")
        .expect("receiver should return SourceDone before late data");
        assert_eq!(source_done_round(&first), Some(0));
        assert_eq!(receiver.pending_control_frames.len(), 0);

        let second = timeout(
            Duration::from_secs(1),
            receiver.next_frame(&mut control_rx, &mut data_rx),
        )
        .await
        .expect("timed out waiting for late data")
        .expect("receiver should return late data after SourceDone");
        assert!(
            lossless_session::decode_block_symbol(&second.bytes).is_some(),
            "FEC SourceDone must not wait behind late data before feedback"
        );
    }

    #[tokio::test]
    async fn fec_receiver_rejects_malformed_symbol_payload_length() {
        let (mut receiver, _packet_rx) =
            fec_test_receiver(8, BTreeSet::new(), BTreeMap::new()).await;

        receiver
            .handle_block_symbol_frame(InboundFrame {
                bytes: lossless_session::encode_block_symbol(
                    receiver.shared.session_id,
                    0,
                    0,
                    0,
                    &[1],
                ),
                peer_id: Some(SOURCE_NODE_ID),
            })
            .await
            .expect("test receiver sink should accept frame");

        assert!(
            receiver
                .mode
                .as_ref()
                .and_then(|mode| match mode {
                    ReceiverMode::Fec(fec) => fec.blocks.get(&0),
                    ReceiverMode::Plain(_)
                    | ReceiverMode::Cloudcast(_)
                    | ReceiverMode::MettleCarousel(_) => None,
                })
                .is_none(),
            "malformed FEC symbol payloads must be dropped before insertion"
        );
    }

    #[tokio::test]
    async fn fec_receiver_rejects_out_of_range_peer_esi_before_storage() {
        let (mut receiver, _packet_rx) =
            fec_test_receiver(8, BTreeSet::new(), BTreeMap::new()).await;

        receiver
            .handle_block_symbol_frame(InboundFrame {
                bytes: lossless_session::encode_block_symbol(
                    receiver.shared.session_id,
                    0,
                    crate::node::session::fec::RAPTORQ_SYMBOL_ID_END_EXCLUSIVE,
                    0,
                    &[1, 2],
                ),
                peer_id: Some(SOURCE_NODE_ID),
            })
            .await
            .expect("test receiver sink should accept frame");

        assert!(
            receiver
                .mode
                .as_ref()
                .and_then(|mode| match mode {
                    ReceiverMode::Fec(fec) => fec.blocks.get(&0),
                    ReceiverMode::Plain(_)
                    | ReceiverMode::Cloudcast(_)
                    | ReceiverMode::MettleCarousel(_) => None,
                })
                .is_none(),
            "out-of-range peer ESIs must be dropped before storage or decoder input"
        );
    }

    #[tokio::test]
    async fn fec_receiver_metrics_count_duplicates_decode_overhead_and_completion_tail() {
        let (mut receiver, _packet_rx) =
            fec_test_receiver(8, BTreeSet::new(), BTreeMap::new()).await;
        let session_id = receiver.shared.session_id;

        for (symbol_id, payload) in [
            (0, [0, 1]),
            (0, [0, 1]),
            (1, [2, 3]),
            (2, [4, 5]),
            (3, [6, 7]),
        ] {
            receiver
                .handle_block_symbol_frame(InboundFrame {
                    bytes: lossless_session::encode_block_symbol(
                        session_id, 0, symbol_id, 0, &payload,
                    ),
                    peer_id: Some(SOURCE_NODE_ID),
                })
                .await
                .expect("test receiver sink should accept frame");
        }

        assert!(receiver.shared.has_all_blocks());
        receiver
            .handle_block_symbol_frame(InboundFrame {
                bytes: lossless_session::encode_block_symbol(session_id, 0, 4, 1, &[8, 9]),
                peer_id: Some(SOURCE_NODE_ID),
            })
            .await
            .expect("post-completion symbol should be accounted and ignored");

        let snapshot = receiver.shared.metrics.snapshot();
        assert_eq!(snapshot.receiver_duplicate_symbols, 1);
        assert_eq!(snapshot.symbols_received_after_local_block_complete, 1);
        assert_eq!(snapshot.symbols_at_decode_minus_k, BTreeMap::from([(0, 1)]));
    }

    #[tokio::test]
    async fn plain_receiver_reports_complete_on_source_done() {
        let (mut receiver, mut packet_rx) = plain_test_receiver(2, BTreeSet::from([0, 1])).await;

        receiver
            .handle_control_frame(InboundFrame {
                bytes: lossless_session::encode_control(
                    receiver.shared.session_id,
                    &LosslessSessionControl::SourceDone { round_id: 0 },
                ),
                peer_id: Some(SOURCE_NODE_ID),
            })
            .await
            .expect("test receiver sink should accept frame");

        assert_eq!(recv_plain_need(&mut packet_rx).await, NeedReport::Complete);
        assert!(receiver.is_complete());
        assert_eq!(receiver.shared.plain_need(), Some(NeedReport::Complete));
    }

    #[tokio::test]
    async fn plain_receiver_reports_sparse_missing_ranges() {
        let (receiver, _packet_rx) = plain_test_receiver(4, BTreeSet::from([0, 2])).await;

        assert_eq!(
            receiver.shared.plain_need(),
            Some(NeedReport::Plain {
                ranges: vec![
                    MissingBlockRange {
                        start_block_id: 1,
                        end_block_id: 2,
                    },
                    MissingBlockRange {
                        start_block_id: 3,
                        end_block_id: 4,
                    },
                ],
            })
        );
    }

    #[tokio::test]
    async fn plain_receiver_emits_sparse_missing_ranges_on_source_done() {
        let (mut receiver, mut packet_rx) = plain_test_receiver(4, BTreeSet::from([0, 2])).await;
        let expected = NeedReport::Plain {
            ranges: vec![
                MissingBlockRange {
                    start_block_id: 1,
                    end_block_id: 2,
                },
                MissingBlockRange {
                    start_block_id: 3,
                    end_block_id: 4,
                },
            ],
        };

        receiver
            .handle_control_frame(InboundFrame {
                bytes: lossless_session::encode_control(
                    receiver.shared.session_id,
                    &LosslessSessionControl::SourceDone { round_id: 0 },
                ),
                peer_id: Some(SOURCE_NODE_ID),
            })
            .await
            .expect("test receiver sink should accept frame");

        assert_eq!(recv_plain_need(&mut packet_rx).await, expected);
        assert!(!receiver.is_complete());
    }

    #[tokio::test]
    async fn plain_receiver_keeps_missing_status_stable_across_repeated_source_done() {
        let (mut receiver, mut packet_rx) = plain_test_receiver(2, BTreeSet::from([0])).await;

        let source_done = InboundFrame {
            bytes: lossless_session::encode_control(
                receiver.shared.session_id,
                &LosslessSessionControl::SourceDone { round_id: 0 },
            ),
            peer_id: Some(SOURCE_NODE_ID),
        };
        let expected = NeedReport::Plain {
            ranges: vec![MissingBlockRange {
                start_block_id: 1,
                end_block_id: 2,
            }],
        };

        receiver
            .handle_control_frame(source_done.clone())
            .await
            .expect("test receiver sink should accept frame");
        assert_eq!(recv_plain_need(&mut packet_rx).await, expected.clone());
        assert!(!receiver.is_complete());
        assert_eq!(receiver.shared.plain_need(), Some(expected.clone()));

        receiver
            .handle_block_data_frame(InboundFrame {
                bytes: lossless_session::encode_block_data(
                    receiver.shared.session_id,
                    1,
                    b"ijklmnop",
                ),
                peer_id: Some(SOURCE_NODE_ID),
            })
            .await
            .expect("test receiver sink should accept frame");

        receiver
            .handle_control_frame(source_done)
            .await
            .expect("test receiver sink should accept frame");
        assert_eq!(recv_plain_need(&mut packet_rx).await, expected.clone());
        assert!(!receiver.is_complete());
    }

    #[tokio::test]
    async fn cloudcast_receiver_reports_need_on_source_done() {
        let (mut receiver, mut packet_rx) = plain_test_receiver(1, BTreeSet::from([0])).await;
        receiver.shared.cfg.cloudcast = Some(
            crate::node::session::runtime::CloudcastRuntimeConfig::new(vec![0, 1], vec![0, 1]),
        );
        receiver.mode = Some(ReceiverMode::Cloudcast(CloudcastReceiver::new(&[0, 1])));

        receiver
            .handle_control_frame(InboundFrame {
                bytes: lossless_session::encode_control(
                    receiver.shared.session_id,
                    &LosslessSessionControl::SourceDone { round_id: 0 },
                ),
                peer_id: Some(SOURCE_NODE_ID),
            })
            .await
            .expect("test receiver sink should accept frame");

        assert_eq!(recv_plain_need(&mut packet_rx).await, NeedReport::Complete);
        assert!(receiver.is_complete());
    }

    #[tokio::test]
    async fn cloudcast_receiver_waits_for_in_flight_blocks_after_source_done() {
        let (mut receiver, mut packet_rx) = plain_test_receiver(2, BTreeSet::from([0])).await;
        receiver.shared.cfg.cloudcast = Some(
            crate::node::session::runtime::CloudcastRuntimeConfig::new(vec![0, 1], vec![0, 1]),
        );
        receiver.mode = Some(ReceiverMode::Cloudcast(CloudcastReceiver::new(&[0, 1])));

        receiver
            .handle_control_frame(InboundFrame {
                bytes: lossless_session::encode_control(
                    receiver.shared.session_id,
                    &LosslessSessionControl::SourceDone { round_id: 0 },
                ),
                peer_id: Some(SOURCE_NODE_ID),
            })
            .await
            .expect("test receiver sink should accept frame");

        assert!(
            timeout(Duration::from_millis(100), packet_rx.recv())
                .await
                .is_err(),
            "Cloudcast SourceDone must not trigger a missing-block Need while tree data is still in flight"
        );

        receiver
            .handle_block_data_frame(InboundFrame {
                bytes: lossless_session::encode_block_data(
                    receiver.shared.session_id,
                    1,
                    b"abcdefgh",
                ),
                peer_id: Some(SOURCE_NODE_ID),
            })
            .await
            .expect("test receiver sink should accept frame");

        assert_eq!(recv_plain_need(&mut packet_rx).await, NeedReport::Complete);
        assert!(receiver.is_complete());
    }

    #[tokio::test]
    async fn plain_receiver_drops_stale_source_done() {
        let (mut receiver, mut packet_rx) = plain_test_receiver(2, BTreeSet::from([0])).await;

        receiver
            .handle_control_frame(InboundFrame {
                bytes: lossless_session::encode_control(
                    receiver.shared.session_id,
                    &LosslessSessionControl::SourceDone { round_id: 1 },
                ),
                peer_id: Some(SOURCE_NODE_ID),
            })
            .await
            .expect("test receiver sink should accept frame");
        assert_eq!(
            recv_plain_need(&mut packet_rx).await,
            NeedReport::Plain {
                ranges: vec![MissingBlockRange {
                    start_block_id: 1,
                    end_block_id: 2,
                }],
            }
        );

        receiver
            .handle_control_frame(InboundFrame {
                bytes: lossless_session::encode_control(
                    receiver.shared.session_id,
                    &LosslessSessionControl::SourceDone { round_id: 0 },
                ),
                peer_id: Some(SOURCE_NODE_ID),
            })
            .await
            .expect("test receiver sink should accept frame");
        assert!(
            timeout(Duration::from_millis(100), packet_rx.recv())
                .await
                .is_err(),
            "stale SourceDone must be dropped"
        );
    }

    #[tokio::test]
    async fn fec_receiver_drops_stale_source_done() {
        let (mut receiver, mut packet_rx) = fec_test_receiver(
            8,
            BTreeSet::new(),
            BTreeMap::from([(0, BTreeMap::from([(0, vec![1, 2])]))]),
        )
        .await;

        receiver
            .handle_control_frame(InboundFrame {
                bytes: lossless_session::encode_control(
                    receiver.shared.session_id,
                    &LosslessSessionControl::SourceDone { round_id: 1 },
                ),
                peer_id: Some(SOURCE_NODE_ID),
            })
            .await
            .expect("test receiver sink should accept frame");
        assert_eq!(
            recv_fec_need(&mut packet_rx).await,
            NeedReport::Fec {
                blocks: vec![NeedBlock {
                    block_id: 0,
                    deficit_symbols: 3,
                }],
            }
        );

        receiver
            .handle_control_frame(InboundFrame {
                bytes: lossless_session::encode_control(
                    receiver.shared.session_id,
                    &LosslessSessionControl::SourceDone { round_id: 0 },
                ),
                peer_id: Some(SOURCE_NODE_ID),
            })
            .await
            .expect("test receiver sink should accept frame");
        assert!(
            timeout(Duration::from_millis(100), packet_rx.recv())
                .await
                .is_err(),
            "stale SourceDone must be dropped"
        );
    }

    #[tokio::test]
    async fn plain_receiver_ignores_duplicate_data_before_source_done() {
        let (mut receiver, mut packet_rx) = plain_test_receiver(2, BTreeSet::from([0])).await;
        let frame = InboundFrame {
            bytes: lossless_session::encode_block_data(receiver.shared.session_id, 0, b"abcdefgh"),
            peer_id: Some(SOURCE_NODE_ID),
        };

        receiver
            .handle_block_data_frame(frame.clone())
            .await
            .expect("test receiver sink should accept frame");
        receiver
            .handle_block_data_frame(frame)
            .await
            .expect("test receiver sink should accept frame");

        assert!(
            timeout(Duration::from_millis(100), packet_rx.recv())
                .await
                .is_err(),
            "plain receiver should not emit per-block feedback before SourceDone"
        );
        assert!(!receiver.is_complete());
        assert_eq!(
            receiver.shared.plain_need(),
            Some(NeedReport::Plain {
                ranges: vec![MissingBlockRange {
                    start_block_id: 1,
                    end_block_id: 2,
                }],
            })
        );
    }

    #[tokio::test]
    async fn plain_receiver_reports_complete_for_zero_byte_object_on_source_done() {
        let (mut receiver, mut packet_rx) = plain_test_receiver(0, BTreeSet::new()).await;

        receiver
            .handle_control_frame(InboundFrame {
                bytes: lossless_session::encode_control(
                    receiver.shared.session_id,
                    &LosslessSessionControl::SourceDone { round_id: 0 },
                ),
                peer_id: Some(SOURCE_NODE_ID),
            })
            .await
            .expect("test receiver sink should accept frame");

        assert_eq!(recv_plain_need(&mut packet_rx).await, NeedReport::Complete);
        assert!(receiver.is_complete());
    }

    #[tokio::test]
    async fn fec_receiver_reports_complete_for_zero_byte_object_on_source_done() {
        let (mut receiver, mut packet_rx) =
            fec_test_receiver(0, BTreeSet::new(), BTreeMap::new()).await;

        receiver
            .handle_control_frame(InboundFrame {
                bytes: lossless_session::encode_control(
                    receiver.shared.session_id,
                    &LosslessSessionControl::SourceDone { round_id: 0 },
                ),
                peer_id: Some(SOURCE_NODE_ID),
            })
            .await
            .expect("test receiver sink should accept frame");

        assert_eq!(recv_fec_need(&mut packet_rx).await, NeedReport::Complete);
        assert!(receiver.is_complete());
    }

    #[tokio::test]
    async fn mark_first_payload_unit_records_progress() {
        let progress = Arc::new(crate::node::session::runtime::ReceiverProgress::default());
        let shared = ReceiverShared {
            session_id: 9,
            route: crate::node::session::runtime::TransportRoute {
                src_ip: std::net::Ipv4Addr::new(10, 0, 0, 1),
                dst_ip: std::net::Ipv4Addr::new(10, 0, 0, 2),
                src_port: 1,
                dst_port: 2,
            },
            local_node_id: 1,
            cfg: ReceiverConfig {
                session_id: 9,
                route: crate::node::session::runtime::TransportRoute {
                    src_ip: std::net::Ipv4Addr::new(10, 0, 0, 1),
                    dst_ip: std::net::Ipv4Addr::new(10, 0, 0, 2),
                    src_port: 1,
                    dst_port: 2,
                },
                local_node_id: 1,
                sink_buffer: None,
                sink_file: None,
                progress: Some(progress.clone()),
                peer_report_timeout_ms: 200,
                fec_enabled: false,
                cloudcast: None,
                carousel: Default::default(),
                mettle_decoder_budget: None,
            },
            processors: crate::node::processor::ProcessorHandle::new(Default::default()),
            manifest: Some(LosslessSessionManifest {
                block_size: 8,
                total_bytes: 8,
                total_blocks: 1,
                mode: LosslessSessionMode::Plain,
            }),
            plan: BlockPlan::new(8, 8).ok(),
            complete_blocks: BTreeSet::new(),
            metrics: Arc::new(SessionMetrics::default()),
        };

        shared.mark_first_payload_unit();

        assert!(progress.first_payload_unit_at().is_some());
    }

    #[tokio::test]
    async fn mark_object_complete_records_progress() {
        let progress = Arc::new(crate::node::session::runtime::ReceiverProgress::default());
        let shared = ReceiverShared {
            session_id: 9,
            route: crate::node::session::runtime::TransportRoute {
                src_ip: std::net::Ipv4Addr::new(10, 0, 0, 1),
                dst_ip: std::net::Ipv4Addr::new(10, 0, 0, 2),
                src_port: 1,
                dst_port: 2,
            },
            local_node_id: 1,
            cfg: ReceiverConfig {
                session_id: 9,
                route: crate::node::session::runtime::TransportRoute {
                    src_ip: std::net::Ipv4Addr::new(10, 0, 0, 1),
                    dst_ip: std::net::Ipv4Addr::new(10, 0, 0, 2),
                    src_port: 1,
                    dst_port: 2,
                },
                local_node_id: 1,
                sink_buffer: None,
                sink_file: None,
                progress: Some(progress.clone()),
                peer_report_timeout_ms: 200,
                fec_enabled: false,
                cloudcast: None,
                carousel: Default::default(),
                mettle_decoder_budget: None,
            },
            processors: crate::node::processor::ProcessorHandle::new(Default::default()),
            manifest: Some(LosslessSessionManifest {
                block_size: 8,
                total_bytes: 8,
                total_blocks: 1,
                mode: LosslessSessionMode::Plain,
            }),
            plan: BlockPlan::new(8, 8).ok(),
            complete_blocks: BTreeSet::new(),
            metrics: Arc::new(SessionMetrics::default()),
        };

        shared.mark_object_complete();

        assert!(progress.object_complete_at().is_some());
    }

    #[tokio::test]
    async fn write_symbol_run_updates_contiguous_sink_region() {
        let route = crate::node::session::runtime::TransportRoute {
            src_ip: std::net::Ipv4Addr::new(10, 0, 0, 1),
            dst_ip: std::net::Ipv4Addr::new(10, 0, 0, 2),
            src_port: 1,
            dst_port: 2,
        };
        let tmp_path = std::env::temp_dir().join(format!(
            "nextmini-symbol-run-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("system clock should be after epoch")
                .as_nanos()
        ));
        let sink_buffer = Arc::new(tokio::sync::Mutex::new(vec![0; 10]));
        let sink_file = Arc::new(tokio::sync::Mutex::new(
            std::fs::OpenOptions::new()
                .create(true)
                .truncate(true)
                .read(true)
                .write(true)
                .open(&tmp_path)
                .expect("temp sink file should open"),
        ));
        let shared = ReceiverShared {
            session_id: 9,
            route,
            local_node_id: 1,
            cfg: ReceiverConfig {
                session_id: 9,
                route,
                local_node_id: 1,
                sink_buffer: Some(sink_buffer.clone()),
                sink_file: Some(sink_file.clone()),
                progress: None,
                peer_report_timeout_ms: 200,
                fec_enabled: false,
                cloudcast: None,
                carousel: Default::default(),
                mettle_decoder_budget: None,
            },
            processors: crate::node::processor::ProcessorHandle::new(Default::default()),
            manifest: Some(LosslessSessionManifest {
                block_size: 10,
                total_bytes: 10,
                total_blocks: 1,
                mode: LosslessSessionMode::Plain,
            }),
            plan: BlockPlan::new(10, 10).ok(),
            complete_blocks: BTreeSet::new(),
            metrics: Arc::new(SessionMetrics::default()),
        };

        shared
            .write_symbol_run(
                0,
                SymbolGeometry::new(10, 4).expect("valid symbol geometry"),
                1,
                &[4, 5, 6, 7, 8, 9, 10, 11, 12],
            )
            .await
            .expect("test sinks should accept symbol run");

        assert_eq!(
            sink_buffer.lock().await.as_slice(),
            &[0, 0, 0, 4, 5, 6, 7, 8, 9, 10]
        );
        sink_file
            .lock()
            .await
            .sync_all()
            .expect("temp sink file should sync");
        assert_eq!(
            std::fs::read(&tmp_path).expect("temp sink file should read"),
            vec![0, 0, 0, 4, 5, 6, 7, 8, 9, 10]
        );
        let _ = std::fs::remove_file(tmp_path);
    }

    #[tokio::test]
    async fn sink_write_error_aborts_receiver_with_distinct_outcome() {
        let route = crate::node::session::runtime::TransportRoute {
            src_ip: Ipv4Addr::new(10, 0, 0, 1),
            dst_ip: Ipv4Addr::new(10, 0, 0, 2),
            src_port: 1,
            dst_port: 2,
        };
        let tmp_path = std::env::temp_dir().join(format!(
            "nextmini-read-only-sink-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("system clock should be after epoch")
                .as_nanos()
        ));
        std::fs::write(&tmp_path, [0u8; 8]).expect("temp sink fixture should be created");
        let read_only_sink = Arc::new(tokio::sync::Mutex::new(
            std::fs::OpenOptions::new()
                .read(true)
                .open(&tmp_path)
                .expect("read-only sink fixture should open"),
        ));
        let processors = ProcessorHandle::new(LocalConfig {
            node_id: RECEIVER_NODE_ID,
            n_nodes: SOURCE_NODE_ID.max(RECEIVER_NODE_ID) + 1,
            num_packet_processors: 1,
            channel_capacity: 8,
            user_space_base_addr: Ipv4Addr::new(10, 0, 0, 0),
            local_netmask: Ipv4Addr::new(255, 255, 255, 0),
            ..Default::default()
        });
        let (control_tx, mut control_rx) = mpsc::channel(2);
        let (data_tx, mut data_rx) = mpsc::channel(2);
        let mut receiver = SessionReceiver::new(
            ReceiverConfig {
                session_id: 91,
                route,
                local_node_id: RECEIVER_NODE_ID,
                sink_buffer: None,
                sink_file: Some(read_only_sink),
                progress: None,
                peer_report_timeout_ms: 200,
                fec_enabled: false,
                cloudcast: None,
                carousel: Default::default(),
                mettle_decoder_budget: None,
            },
            processors,
        );

        control_tx
            .send(InboundFrame {
                bytes: lossless_session::encode_control(
                    91,
                    &LosslessSessionControl::Manifest {
                        manifest: LosslessSessionManifest {
                            block_size: 8,
                            total_bytes: 8,
                            total_blocks: 1,
                            mode: LosslessSessionMode::Plain,
                        },
                    },
                ),
                peer_id: Some(SOURCE_NODE_ID),
            })
            .await
            .expect("manifest should reach receiver");
        data_tx
            .send(InboundFrame {
                bytes: lossless_session::encode_block_data(91, 0, b"abcdefgh"),
                peer_id: Some(SOURCE_NODE_ID),
            })
            .await
            .expect("block should reach receiver");

        let outcome = timeout(
            Duration::from_secs(2),
            receiver.run(&mut control_rx, &mut data_rx, None),
        )
        .await
        .expect("receiver should stop after sink write failure");
        assert_eq!(outcome, SessionOutcome::SinkError);
        assert!(receiver.shared.complete_blocks.is_empty());
        assert!(
            !receiver.reported_complete(),
            "a rejected sink write must never produce Complete feedback"
        );

        drop(receiver);
        let _ = std::fs::remove_file(tmp_path);
    }

    #[tokio::test]
    async fn completed_fec_receiver_registers_complete_replay_before_teardown() {
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

        let route = crate::node::session::runtime::TransportRoute {
            src_ip: RECEIVER_NODE_ID.ip_addr(cfg.user_space_base_addr, cfg.local_netmask),
            dst_ip: SOURCE_NODE_ID.ip_addr(cfg.user_space_base_addr, cfg.local_netmask),
            src_port: 4751,
            dst_port: 5751,
        };
        let flow_id =
            Packet::flow_id_from_parts(route.src_ip, route.src_port, route.dst_ip, route.dst_port);
        let (packet_tx, mut packet_rx) = mpsc::channel(8);
        processors.connect_user_space_sender(flow_id, packet_tx);
        tokio::time::sleep(Duration::from_millis(50)).await;

        let geometry = BlockPlan::new(8, 8)
            .ok()
            .and_then(|plan| plan.symbol_geometry(4).ok())
            .expect("valid geometry");
        let mut receiver = SessionReceiver {
            shared: ReceiverShared {
                session_id: 9,
                route,
                local_node_id: RECEIVER_NODE_ID,
                cfg: ReceiverConfig {
                    session_id: 9,
                    route,
                    local_node_id: RECEIVER_NODE_ID,
                    sink_buffer: None,
                    sink_file: None,
                    progress: None,
                    peer_report_timeout_ms: 200,
                    fec_enabled: true,
                    cloudcast: None,
                    carousel: Default::default(),
                    mettle_decoder_budget: None,
                },
                processors,
                manifest: Some(LosslessSessionManifest {
                    block_size: 8,
                    total_bytes: 8,
                    total_blocks: 1,
                    mode: LosslessSessionMode::Fec(
                        nextmini_messages::lossless_session::LosslessSessionFecMode::new_raptorq(
                            4,
                            vec![0, 1],
                        ),
                    ),
                }),
                plan: BlockPlan::new(8, 8).ok(),
                complete_blocks: BTreeSet::from([0]),
                metrics: Arc::new(SessionMetrics::default()),
            },
            mode: Some(ReceiverMode::Fec(FecReceiver::new(
                geometry,
                crate::node::session::fec::FecSymbolIdBounds::raptorq(),
            ))),
            lifecycle: ReceiverLifecycle::Active,
            passive_complete_deadline: None,
            pending_control_frames: VecDeque::new(),
            carousel_ack: None,
        };

        receiver
            .handle_control_frame(InboundFrame {
                bytes: lossless_session::encode_control(
                    receiver.shared.session_id,
                    &LosslessSessionControl::SourceDone { round_id: 0 },
                ),
                peer_id: Some(SOURCE_NODE_ID),
            })
            .await
            .expect("test receiver sink should accept frame");
        assert_eq!(
            recv_fec_need(&mut packet_rx).await,
            NeedReport::Complete,
            "completed FEC receivers should report complete at the round boundary"
        );
        assert!(receiver.is_complete());

        let (runtime_tx, mut runtime_rx) = mpsc::channel(8);
        let (_control_tx, mut control_rx) = mpsc::channel(1);
        let (_data_tx, mut data_rx) = mpsc::channel(1);
        let register_task = tokio::spawn(async move {
            receiver
                .register_completed_replay(Some(runtime_tx), &mut control_rx, &mut data_rx)
                .await;
        });

        let LosslessRuntimeMessage::ReceiverCompleted {
            session_id,
            replay,
            ack,
        } = timeout(Duration::from_secs(2), runtime_rx.recv())
            .await
            .expect("timed out waiting for replay registration")
            .expect("runtime channel closed unexpectedly")
        else {
            panic!("unexpected runtime message");
        };
        assert_eq!(session_id, 9);
        assert!(matches!(
            replay,
            CompletedReceiverReplay::Fec {
                round_id: 0,
                route: replay_route,
                report: NeedReport::Complete,
                ..
            } if replay_route == route
        ));
        ack.send(())
            .expect("replay registration should still await ack");

        register_task
            .await
            .expect("replay registration task should exit cleanly");
    }

    #[tokio::test]
    async fn completed_carousel_receiver_answers_probe_while_replay_install_is_pending() {
        let (mut receiver, mut packet_rx) =
            fec_test_receiver(8, BTreeSet::from([0]), BTreeMap::new()).await;
        let manifest = receiver
            .shared
            .manifest
            .as_mut()
            .expect("test receiver has a manifest");
        let LosslessSessionMode::Fec(fec_mode) = &mut manifest.mode else {
            panic!("test receiver must use FEC");
        };
        fec_mode.feedback_mode = FecFeedbackMode::Carousel;
        receiver.carousel_ack = Some(CarouselAckState::new(
            Instant::now(),
            receiver.shared.cfg.carousel,
        ));
        receiver.lifecycle = ReceiverLifecycle::SessionFinished;
        let session_id = receiver.shared.session_id;

        let (runtime_tx, mut runtime_rx) = mpsc::channel(8);
        let (control_tx, mut control_rx) = mpsc::channel(1);
        let (_data_tx, mut data_rx) = mpsc::channel(1);
        let register_task = tokio::spawn(async move {
            receiver
                .register_completed_replay(Some(runtime_tx), &mut control_rx, &mut data_rx)
                .await;
        });

        let LosslessRuntimeMessage::ReceiverCompleted { replay, ack, .. } =
            timeout(Duration::from_secs(2), runtime_rx.recv())
                .await
                .expect("timed out waiting for carousel replay registration")
                .expect("runtime channel closed unexpectedly")
        else {
            panic!("unexpected runtime message");
        };
        assert!(matches!(replay, CompletedReceiverReplay::Carousel { .. }));

        control_tx
            .send(InboundFrame {
                bytes: lossless_session::encode_control(
                    session_id,
                    &LosslessSessionControl::AckProbe {
                        target_peer_id: u64::try_from(RECEIVER_NODE_ID)
                            .expect("receiver id fits u64"),
                    },
                ),
                peer_id: Some(SOURCE_NODE_ID),
            })
            .await
            .expect("probe should reach receiver during replay handoff");
        assert_eq!(
            recv_block_ack(&mut packet_rx).await,
            BlockAck::Blocks {
                completed_watermark: 1,
                extra_completed: Vec::new(),
            }
        );

        ack.send(())
            .expect("runtime should acknowledge replay install");
        register_task
            .await
            .expect("replay registration task should exit cleanly");
    }

    #[tokio::test]
    async fn passive_complete_receiver_defers_replay_registration_until_session_finish() {
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

        let route = crate::node::session::runtime::TransportRoute {
            src_ip: RECEIVER_NODE_ID.ip_addr(cfg.user_space_base_addr, cfg.local_netmask),
            dst_ip: SOURCE_NODE_ID.ip_addr(cfg.user_space_base_addr, cfg.local_netmask),
            src_port: 4753,
            dst_port: 5753,
        };
        let flow_id =
            Packet::flow_id_from_parts(route.src_ip, route.src_port, route.dst_ip, route.dst_port);
        let (packet_tx, mut packet_rx) = mpsc::channel(8);
        processors.connect_user_space_sender(flow_id, packet_tx);
        tokio::time::sleep(Duration::from_millis(50)).await;

        let (runtime_tx, mut runtime_rx) = mpsc::channel(8);
        let (control_tx, control_rx) = mpsc::channel(8);
        let (data_tx, data_rx) = mpsc::channel(8);
        let receiver_task = tokio::spawn(run_with_runtime(
            ReceiverConfig {
                session_id: 11,
                route,
                local_node_id: RECEIVER_NODE_ID,
                sink_buffer: None,
                sink_file: None,
                progress: None,
                peer_report_timeout_ms: 200,
                fec_enabled: false,
                cloudcast: None,
                carousel: Default::default(),
                mettle_decoder_budget: None,
            },
            control_rx,
            data_rx,
            processors.clone(),
            Some(runtime_tx),
        ));

        control_tx
            .send(InboundFrame {
                bytes: lossless_session::encode_control(
                    11,
                    &LosslessSessionControl::Manifest {
                        manifest: LosslessSessionManifest {
                            block_size: 8,
                            total_bytes: 8,
                            total_blocks: 1,
                            mode: LosslessSessionMode::Plain,
                        },
                    },
                ),
                peer_id: Some(SOURCE_NODE_ID),
            })
            .await
            .expect("manifest should reach receiver");

        let _ready = timeout(Duration::from_secs(2), packet_rx.recv())
            .await
            .expect("timed out waiting for Ready")
            .expect("packet capture closed unexpectedly");

        data_tx
            .send(InboundFrame {
                bytes: lossless_session::encode_block_data(11, 0, b"abcdefgh"),
                peer_id: Some(SOURCE_NODE_ID),
            })
            .await
            .expect("block data should reach receiver");
        control_tx
            .send(InboundFrame {
                bytes: lossless_session::encode_control(
                    11,
                    &LosslessSessionControl::SourceDone { round_id: 0 },
                ),
                peer_id: Some(SOURCE_NODE_ID),
            })
            .await
            .expect("first SourceDone should reach receiver");

        assert_eq!(recv_plain_need(&mut packet_rx).await, NeedReport::Complete);
        assert!(
            timeout(Duration::from_millis(20), runtime_rx.recv())
                .await
                .is_err(),
            "runtime handoff must not start while the passive-complete receiver can still answer later rounds"
        );

        control_tx
            .send(InboundFrame {
                bytes: lossless_session::encode_control(
                    11,
                    &LosslessSessionControl::SourceDone { round_id: 1 },
                ),
                peer_id: Some(SOURCE_NODE_ID),
            })
            .await
            .expect("second SourceDone should reach receiver");

        assert_eq!(recv_plain_need(&mut packet_rx).await, NeedReport::Complete);
        assert!(
            timeout(Duration::from_millis(20), runtime_rx.recv())
                .await
                .is_err(),
            "runtime handoff must still wait while the live passive-complete receiver owns future-round replies"
        );

        drop(control_tx);
        drop(data_tx);

        let LosslessRuntimeMessage::ReceiverCompleted {
            session_id,
            replay,
            ack,
        } = timeout(Duration::from_secs(2), runtime_rx.recv())
            .await
            .expect("timed out waiting for replay handoff")
            .expect("runtime channel closed unexpectedly")
        else {
            panic!("unexpected runtime message");
        };
        assert_eq!(session_id, 11);
        assert!(matches!(
            replay,
            CompletedReceiverReplay::Plain {
                round_id: 1,
                route: replay_route,
                report: NeedReport::Complete,
                ..
            } if replay_route == route
        ));
        ack.send(()).expect("replay handoff should still await ack");

        receiver_task
            .await
            .expect("receiver task should exit cleanly after handoff");
    }

    #[tokio::test]
    async fn passive_complete_receiver_survives_past_peer_report_timeout_to_answer_later_round() {
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

        let route = crate::node::session::runtime::TransportRoute {
            src_ip: RECEIVER_NODE_ID.ip_addr(cfg.user_space_base_addr, cfg.local_netmask),
            dst_ip: SOURCE_NODE_ID.ip_addr(cfg.user_space_base_addr, cfg.local_netmask),
            src_port: 4754,
            dst_port: 5754,
        };
        let flow_id =
            Packet::flow_id_from_parts(route.src_ip, route.src_port, route.dst_ip, route.dst_port);
        let (packet_tx, mut packet_rx) = mpsc::channel(8);
        processors.connect_user_space_sender(flow_id, packet_tx);
        tokio::time::sleep(Duration::from_millis(50)).await;

        let (runtime_tx, mut runtime_rx) = mpsc::channel(8);
        let (control_tx, control_rx) = mpsc::channel(8);
        let (data_tx, data_rx) = mpsc::channel(8);
        let receiver_task = tokio::spawn(run_with_runtime(
            ReceiverConfig {
                session_id: 12,
                route,
                local_node_id: RECEIVER_NODE_ID,
                sink_buffer: None,
                sink_file: None,
                progress: None,
                peer_report_timeout_ms: 200,
                fec_enabled: false,
                cloudcast: None,
                carousel: Default::default(),
                mettle_decoder_budget: None,
            },
            control_rx,
            data_rx,
            processors.clone(),
            Some(runtime_tx),
        ));

        control_tx
            .send(InboundFrame {
                bytes: lossless_session::encode_control(
                    12,
                    &LosslessSessionControl::Manifest {
                        manifest: LosslessSessionManifest {
                            block_size: 8,
                            total_bytes: 8,
                            total_blocks: 1,
                            mode: LosslessSessionMode::Plain,
                        },
                    },
                ),
                peer_id: Some(SOURCE_NODE_ID),
            })
            .await
            .expect("manifest should reach receiver");

        let _ready = timeout(Duration::from_secs(2), packet_rx.recv())
            .await
            .expect("timed out waiting for Ready")
            .expect("packet capture closed unexpectedly");

        data_tx
            .send(InboundFrame {
                bytes: lossless_session::encode_block_data(12, 0, b"abcdefgh"),
                peer_id: Some(SOURCE_NODE_ID),
            })
            .await
            .expect("block data should reach receiver");
        control_tx
            .send(InboundFrame {
                bytes: lossless_session::encode_control(
                    12,
                    &LosslessSessionControl::SourceDone { round_id: 0 },
                ),
                peer_id: Some(SOURCE_NODE_ID),
            })
            .await
            .expect("first SourceDone should reach receiver");

        assert_eq!(recv_plain_need(&mut packet_rx).await, NeedReport::Complete);
        tokio::time::sleep(
            crate::node::session::timing::peer_report_timeout() + Duration::from_millis(25),
        )
        .await;
        assert!(
            matches!(
                runtime_rx.try_recv(),
                Err(tokio::sync::mpsc::error::TryRecvError::Empty)
            ),
            "receiver should remain live past the sender peer-report timeout budget"
        );

        control_tx
            .send(InboundFrame {
                bytes: lossless_session::encode_control(
                    12,
                    &LosslessSessionControl::SourceDone { round_id: 1 },
                ),
                peer_id: Some(SOURCE_NODE_ID),
            })
            .await
            .expect("second SourceDone should reach receiver");
        assert_eq!(recv_plain_need(&mut packet_rx).await, NeedReport::Complete);

        drop(control_tx);
        drop(data_tx);

        let LosslessRuntimeMessage::ReceiverCompleted {
            session_id,
            replay,
            ack,
        } = timeout(Duration::from_secs(2), runtime_rx.recv())
            .await
            .expect("timed out waiting for replay handoff")
            .expect("runtime channel closed unexpectedly")
        else {
            panic!("unexpected runtime message");
        };
        assert_eq!(session_id, 12);
        assert!(matches!(
            replay,
            CompletedReceiverReplay::Plain {
                round_id: 1,
                route: replay_route,
                report: NeedReport::Complete,
                ..
            } if replay_route == route
        ));
        ack.send(()).expect("replay handoff should still await ack");

        receiver_task
            .await
            .expect("receiver task should exit cleanly after handoff");
    }

    async fn plain_test_receiver(
        total_blocks: u64,
        complete_blocks: BTreeSet<u64>,
    ) -> (SessionReceiver, mpsc::Receiver<Packet>) {
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

        let route = crate::node::session::runtime::TransportRoute {
            src_ip: RECEIVER_NODE_ID.ip_addr(cfg.user_space_base_addr, cfg.local_netmask),
            dst_ip: SOURCE_NODE_ID.ip_addr(cfg.user_space_base_addr, cfg.local_netmask),
            src_port: 4750,
            dst_port: 5750,
        };
        let flow_id =
            Packet::flow_id_from_parts(route.src_ip, route.src_port, route.dst_ip, route.dst_port);
        let (packet_tx, packet_rx) = mpsc::channel(8);
        processors.connect_user_space_sender(flow_id, packet_tx);
        tokio::time::sleep(Duration::from_millis(50)).await;

        (
            SessionReceiver {
                shared: ReceiverShared {
                    session_id: 8,
                    route,
                    local_node_id: RECEIVER_NODE_ID,
                    cfg: ReceiverConfig {
                        session_id: 8,
                        route,
                        local_node_id: RECEIVER_NODE_ID,
                        sink_buffer: None,
                        sink_file: None,
                        progress: None,
                        peer_report_timeout_ms: 200,
                        fec_enabled: false,
                        cloudcast: None,
                        carousel: Default::default(),
                        mettle_decoder_budget: None,
                    },
                    processors,
                    manifest: Some(LosslessSessionManifest {
                        block_size: 8,
                        total_bytes: total_blocks * 8,
                        total_blocks,
                        mode: LosslessSessionMode::Plain,
                    }),
                    plan: BlockPlan::new(total_blocks * 8, 8).ok(),
                    complete_blocks,
                    metrics: Arc::new(SessionMetrics::default()),
                },
                mode: Some(ReceiverMode::Plain(PlainReceiver::default())),
                lifecycle: ReceiverLifecycle::Active,
                passive_complete_deadline: None,
                pending_control_frames: VecDeque::new(),
                carousel_ack: None,
            },
            packet_rx,
        )
    }

    fn test_fec_symbol_id_bounds(
        manifest: &LosslessSessionManifest,
    ) -> crate::node::session::fec::FecSymbolIdBounds {
        let LosslessSessionMode::Fec(fec_mode) = &manifest.mode else {
            panic!("test manifest must use FEC mode");
        };
        crate::node::session::fec::validate_fec_geometry(manifest.block_size, fec_mode)
            .expect("valid test FEC geometry")
            .symbol_id_bounds()
    }

    async fn fec_test_receiver(
        total_bytes: u64,
        complete_blocks: BTreeSet<u64>,
        blocks: BTreeMap<u64, BTreeMap<u32, Vec<u8>>>,
    ) -> (SessionReceiver, mpsc::Receiver<Packet>) {
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

        let route = crate::node::session::runtime::TransportRoute {
            src_ip: RECEIVER_NODE_ID.ip_addr(cfg.user_space_base_addr, cfg.local_netmask),
            dst_ip: SOURCE_NODE_ID.ip_addr(cfg.user_space_base_addr, cfg.local_netmask),
            src_port: 4752,
            dst_port: 5752,
        };
        let flow_id =
            Packet::flow_id_from_parts(route.src_ip, route.src_port, route.dst_ip, route.dst_port);
        let (packet_tx, packet_rx) = mpsc::channel(8);
        processors.connect_user_space_sender(flow_id, packet_tx);
        tokio::time::sleep(Duration::from_millis(50)).await;

        let plan = BlockPlan::new(total_bytes, 8).expect("valid plan");
        let geometry = plan.symbol_geometry(4).expect("valid geometry");

        let mut fec = FecReceiver::new(
            geometry,
            crate::node::session::fec::FecSymbolIdBounds::raptorq(),
        );
        fec.blocks = blocks
            .into_iter()
            .map(|(block_id, symbols)| {
                (
                    block_id,
                    FecBlockState {
                        symbols,
                        ..Default::default()
                    },
                )
            })
            .collect();

        (
            SessionReceiver {
                shared: ReceiverShared {
                    session_id: 10,
                    route,
                    local_node_id: RECEIVER_NODE_ID,
                    cfg: ReceiverConfig {
                        session_id: 10,
                        route,
                        local_node_id: RECEIVER_NODE_ID,
                        sink_buffer: None,
                        sink_file: None,
                        progress: None,
                        peer_report_timeout_ms: 200,
                        fec_enabled: true,
                        cloudcast: None,
                        carousel: Default::default(),
                        mettle_decoder_budget: None,
                    },
                    processors,
                    manifest: Some(LosslessSessionManifest {
                        block_size: 8,
                        total_bytes,
                        total_blocks: plan.total_blocks(),
                        mode: LosslessSessionMode::Fec(
                            nextmini_messages::lossless_session::LosslessSessionFecMode::new_raptorq(
                                4,
                                vec![0, 1],
                            ),
                        ),
                    }),
                    plan: Some(plan),
                    complete_blocks,
                    metrics: Arc::new(SessionMetrics::default()),
                },
                mode: Some(ReceiverMode::Fec(fec)),
                lifecycle: ReceiverLifecycle::Active,
                passive_complete_deadline: None,
                pending_control_frames: VecDeque::new(),
                carousel_ack: None,
            },
            packet_rx,
        )
    }

    async fn carousel_test_receiver(
        complete_blocks: BTreeSet<u64>,
    ) -> (SessionReceiver, mpsc::Receiver<Packet>) {
        let (mut receiver, packet_rx) =
            fec_test_receiver(8, complete_blocks, BTreeMap::new()).await;
        let manifest = receiver
            .shared
            .manifest
            .as_mut()
            .expect("test receiver has a manifest");
        let LosslessSessionMode::Fec(fec) = &mut manifest.mode else {
            panic!("test receiver must use FEC");
        };
        fec.feedback_mode = FecFeedbackMode::Carousel;
        receiver.shared.cfg.carousel = crate::node::session::runtime::CarouselRuntimeConfig {
            ack_debounce: Duration::from_millis(15),
            ack_heartbeat: Duration::from_millis(60),
            ack_probe_interval: Duration::from_millis(30),
            peer_silence_timeout: Duration::from_millis(120),
            peer_stall_timeout: Duration::from_millis(200),
            receiver_passive_window: Duration::from_millis(300),
            ..Default::default()
        };
        receiver.carousel_ack = Some(CarouselAckState::new(
            Instant::now(),
            receiver.shared.cfg.carousel,
        ));
        (receiver, packet_rx)
    }

    async fn recv_block_ack(packet_rx: &mut mpsc::Receiver<Packet>) -> BlockAck {
        let packet = timeout(Duration::from_secs(2), packet_rx.recv())
            .await
            .expect("timed out waiting for BlockAck")
            .expect("packet capture closed unexpectedly");
        let payload = packet
            .tcp_payload()
            .expect("BlockAck packet should include payload");
        let (_, control) =
            lossless_session::decode_control(payload).expect("BlockAck should decode");
        let LosslessSessionControl::BlockAck { ack } = control else {
            panic!("unexpected control frame: {control:?}");
        };
        ack
    }

    async fn recv_plain_need(packet_rx: &mut mpsc::Receiver<Packet>) -> NeedReport {
        let packet = timeout(Duration::from_secs(2), packet_rx.recv())
            .await
            .expect("timed out waiting for plain need")
            .expect("packet capture closed unexpectedly");
        let payload = packet
            .tcp_payload()
            .expect("plain need packet should include payload");
        let (_, control) =
            lossless_session::decode_control(payload).expect("plain need should decode");
        let LosslessSessionControl::Need { report, .. } = control else {
            panic!("unexpected control frame: {control:?}");
        };
        report
    }

    async fn recv_fec_need(packet_rx: &mut mpsc::Receiver<Packet>) -> NeedReport {
        let packet = timeout(Duration::from_secs(2), packet_rx.recv())
            .await
            .expect("timed out waiting for fec need")
            .expect("packet capture closed unexpectedly");
        let payload = packet
            .tcp_payload()
            .expect("fec need packet should include payload");
        let (_, control) =
            lossless_session::decode_control(payload).expect("fec need should decode");
        let LosslessSessionControl::Need { report, .. } = control else {
            panic!("unexpected control frame: {control:?}");
        };
        report
    }
}
