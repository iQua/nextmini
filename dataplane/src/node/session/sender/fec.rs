use bytes::Bytes;
use std::collections::BTreeMap;
use std::num::NonZeroUsize;
use std::time::{Duration, Instant};
use tokio::sync::mpsc;
use tracing::{debug, info, warn};

use nextmini_messages::lossless_session::{
    FecScheme, LosslessSessionManifest, LosslessSessionMode, NeedReport,
};

use crate::node::processor::SendOutcome;
use crate::node::session::api::InboundFrame;
use crate::node::session::api::SessionOutcome;
use crate::node::session::control;
use crate::node::session::fec as session_fec;
use crate::node::session::fec::{BlockParams, Encoder};
use crate::node::session::plan::{BlockPlan, BlockSpan, SymbolGeometry};

use super::block_symbol_frame;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RoundPhase {
    SendingData,
    WaitingForReports,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SymbolKind {
    Source,
    Repair,
}

const FEC_SENDER_PROGRESS_LOG_INTERVAL: Duration = Duration::from_secs(1);
const FEC_SENDER_PROGRESS_SYMBOL_INTERVAL: u64 = 4096;

/// FEC-mode sender state and scheduling cursors.
pub(super) struct FecSender {
    blocks: Vec<FecBlockState>,
    scheme: FecScheme,
    symbols_per_block: u32,
    initial_symbol_count: u32,
    tree_ids: Vec<u16>,
    geometry: SymbolGeometry,
    next_tree_rr: usize,
    current_source_cache: Option<(u64, Vec<Bytes>)>,
    frame_scratch: Vec<u8>,
    round_source_done_sent: bool,
    current_round_id: u32,
    phase: RoundPhase,
    round_complete: bool,
    round_reports: BTreeMap<usize, NeedReport>,
    repair_window_symbols: u32,
    protocol_error: bool,
    stats: FecSenderStats,
}

/// Per-block sender cursor and encoder state for FEC mode.
struct FecBlockState {
    next_source_symbol: u32,
    next_fountain_symbol: u32,
    required_extra_symbols: u16,
    emitted_extra_symbols: u16,
    encoder: Option<Encoder>,
    mettle_stream: Option<MettleSymbolStream>,
}

/// Per-block streaming METTLE encoder state.
///
/// It keeps only the paper encoder's open coupling window plus any finalized
/// bins emitted ahead of the currently retried symbol.
struct MettleSymbolStream {
    encoder: Option<mettle::stream::Encoder>,
    span: BlockSpan,
    geometry: SymbolGeometry,
    terminal_source_count: u64,
    next_source_id: u64,
    finished: bool,
    buffered_bins: BTreeMap<u32, Vec<u8>>,
    advance_calls: u64,
    source_symbols_pushed: u64,
    bins_buffered_total: u64,
    finish_calls: u64,
}

impl MettleSymbolStream {
    fn new(
        span: BlockSpan,
        geometry: SymbolGeometry,
        seed: u64,
        terminal_source_count: u64,
    ) -> Option<Self> {
        let source_symbol_bytes = NonZeroUsize::new(geometry.symbol_size())?;
        Some(Self {
            encoder: Some(mettle::stream::Encoder::new_terminated(
                mettle::MettleParams::new(mettle::OverheadRatio::DEFAULT),
                source_symbol_bytes,
                seed,
                terminal_source_count,
            )),
            span,
            geometry,
            terminal_source_count,
            next_source_id: 0,
            finished: false,
            buffered_bins: BTreeMap::new(),
            advance_calls: 0,
            source_symbols_pushed: 0,
            bins_buffered_total: 0,
            finish_calls: 0,
        })
    }

    fn symbol_payload(&mut self, source: &super::BlockSource, symbol_id: u32) -> Option<Vec<u8>> {
        while !self.buffered_bins.contains_key(&symbol_id) {
            if !self.advance(source)? {
                break;
            }
        }
        self.buffered_bins.get(&symbol_id).cloned()
    }

    fn advance(&mut self, source: &super::BlockSource) -> Option<bool> {
        self.advance_calls = self.advance_calls.saturating_add(1);
        if self.next_source_id < self.terminal_source_count {
            let source_index = usize::try_from(self.next_source_id).ok()?;
            let payload = source.source_symbol_payload(self.span, self.geometry, source_index)?;
            let bins = self.encoder.as_mut()?.push_source(&payload);
            self.next_source_id += 1;
            self.source_symbols_pushed = self.source_symbols_pushed.saturating_add(1);
            self.buffer_bins(bins)?;
            return Some(true);
        }

        if !self.finished {
            let bins = self.encoder.take()?.finish();
            self.finished = true;
            self.finish_calls = self.finish_calls.saturating_add(1);
            self.buffer_bins(bins)?;
            return Some(true);
        }

        Some(false)
    }

    fn buffer_bins(&mut self, bins: Vec<mettle::stream::EncodedBin>) -> Option<()> {
        self.bins_buffered_total = self
            .bins_buffered_total
            .saturating_add(u64::try_from(bins.len()).unwrap_or(u64::MAX));
        for bin in bins {
            let (bin_id, payload) = bin.into_parts();
            let bin_id = u32::try_from(bin_id).ok()?;
            self.buffered_bins.insert(bin_id, payload);
        }
        Some(())
    }

    fn discard_through(&mut self, symbol_id: u32) {
        let Some(next_symbol_id) = symbol_id.checked_add(1) else {
            self.buffered_bins.clear();
            return;
        };
        self.buffered_bins = self.buffered_bins.split_off(&next_symbol_id);
    }

    #[cfg(test)]
    fn buffered_bin_count(&self) -> usize {
        self.buffered_bins.len()
    }
}

#[derive(Debug)]
struct FecSenderStats {
    per_tree: BTreeMap<u16, FecTreeSendStats>,
    source_attempts: u64,
    source_queued: u64,
    source_would_block: u64,
    source_closed: u64,
    repair_attempts: u64,
    repair_queued: u64,
    repair_would_block: u64,
    repair_closed: u64,
    source_send_stalls: u64,
    repair_send_stalls: u64,
    source_payload_builds: u64,
    source_payload_build_nanos: u128,
    repair_payload_builds: u64,
    repair_payload_build_nanos: u128,
    raptorq_encoder_builds: u64,
    raptorq_encoder_build_nanos: u128,
    raptorq_coded_symbol_builds: u64,
    raptorq_coded_symbol_nanos: u128,
    mettle_symbol_requests: u64,
    mettle_symbol_request_nanos: u128,
    last_progress_at: Instant,
    last_progress_queued: u64,
    last_progress_attempts: u64,
    last_progress_would_block: u64,
}

#[derive(Debug, Default)]
struct FecTreeSendStats {
    source_attempts: u64,
    source_queued: u64,
    source_would_block: u64,
    source_closed: u64,
    repair_attempts: u64,
    repair_queued: u64,
    repair_would_block: u64,
    repair_closed: u64,
}

impl FecSenderStats {
    fn new(tree_ids: &[u16]) -> Self {
        Self {
            per_tree: tree_ids
                .iter()
                .copied()
                .map(|tree_id| (tree_id, FecTreeSendStats::default()))
                .collect(),
            source_attempts: 0,
            source_queued: 0,
            source_would_block: 0,
            source_closed: 0,
            repair_attempts: 0,
            repair_queued: 0,
            repair_would_block: 0,
            repair_closed: 0,
            source_send_stalls: 0,
            repair_send_stalls: 0,
            source_payload_builds: 0,
            source_payload_build_nanos: 0,
            repair_payload_builds: 0,
            repair_payload_build_nanos: 0,
            raptorq_encoder_builds: 0,
            raptorq_encoder_build_nanos: 0,
            raptorq_coded_symbol_builds: 0,
            raptorq_coded_symbol_nanos: 0,
            mettle_symbol_requests: 0,
            mettle_symbol_request_nanos: 0,
            last_progress_at: Instant::now(),
            last_progress_queued: 0,
            last_progress_attempts: 0,
            last_progress_would_block: 0,
        }
    }

    fn record_attempt(&mut self, kind: SymbolKind, tree_id: u16) {
        let tree = self.per_tree.entry(tree_id).or_default();
        match kind {
            SymbolKind::Source => {
                self.source_attempts = self.source_attempts.saturating_add(1);
                tree.source_attempts = tree.source_attempts.saturating_add(1);
            }
            SymbolKind::Repair => {
                self.repair_attempts = self.repair_attempts.saturating_add(1);
                tree.repair_attempts = tree.repair_attempts.saturating_add(1);
            }
        }
    }

    fn record_payload_build(&mut self, kind: SymbolKind, duration: Duration) {
        match kind {
            SymbolKind::Source => {
                self.source_payload_builds = self.source_payload_builds.saturating_add(1);
                self.source_payload_build_nanos = self
                    .source_payload_build_nanos
                    .saturating_add(duration.as_nanos());
            }
            SymbolKind::Repair => {
                self.repair_payload_builds = self.repair_payload_builds.saturating_add(1);
                self.repair_payload_build_nanos = self
                    .repair_payload_build_nanos
                    .saturating_add(duration.as_nanos());
            }
        }
    }

    fn record_raptorq_encoder_build(&mut self, duration: Duration) {
        self.raptorq_encoder_builds = self.raptorq_encoder_builds.saturating_add(1);
        self.raptorq_encoder_build_nanos = self
            .raptorq_encoder_build_nanos
            .saturating_add(duration.as_nanos());
    }

    fn record_raptorq_coded_symbol(&mut self, duration: Duration) {
        self.raptorq_coded_symbol_builds = self.raptorq_coded_symbol_builds.saturating_add(1);
        self.raptorq_coded_symbol_nanos = self
            .raptorq_coded_symbol_nanos
            .saturating_add(duration.as_nanos());
    }

    fn record_mettle_symbol_request(&mut self, duration: Duration) {
        self.mettle_symbol_requests = self.mettle_symbol_requests.saturating_add(1);
        self.mettle_symbol_request_nanos = self
            .mettle_symbol_request_nanos
            .saturating_add(duration.as_nanos());
    }

    fn record_outcome(&mut self, kind: SymbolKind, tree_id: u16, outcome: SendOutcome) {
        let tree = self.per_tree.entry(tree_id).or_default();
        match (kind, outcome) {
            (SymbolKind::Source, SendOutcome::Queued) => {
                self.source_queued = self.source_queued.saturating_add(1);
                tree.source_queued = tree.source_queued.saturating_add(1);
            }
            (SymbolKind::Source, SendOutcome::WouldBlock) => {
                self.source_would_block = self.source_would_block.saturating_add(1);
                tree.source_would_block = tree.source_would_block.saturating_add(1);
            }
            (SymbolKind::Source, SendOutcome::Closed) => {
                self.source_closed = self.source_closed.saturating_add(1);
                tree.source_closed = tree.source_closed.saturating_add(1);
            }
            (SymbolKind::Repair, SendOutcome::Queued) => {
                self.repair_queued = self.repair_queued.saturating_add(1);
                tree.repair_queued = tree.repair_queued.saturating_add(1);
            }
            (SymbolKind::Repair, SendOutcome::WouldBlock) => {
                self.repair_would_block = self.repair_would_block.saturating_add(1);
                tree.repair_would_block = tree.repair_would_block.saturating_add(1);
            }
            (SymbolKind::Repair, SendOutcome::Closed) => {
                self.repair_closed = self.repair_closed.saturating_add(1);
                tree.repair_closed = tree.repair_closed.saturating_add(1);
            }
        }
    }

    fn record_stall(&mut self, kind: SymbolKind) {
        match kind {
            SymbolKind::Source => {
                self.source_send_stalls = self.source_send_stalls.saturating_add(1);
            }
            SymbolKind::Repair => {
                self.repair_send_stalls = self.repair_send_stalls.saturating_add(1);
            }
        }
    }

    fn total_queued(&self) -> u64 {
        self.source_queued.saturating_add(self.repair_queued)
    }

    fn total_attempts(&self) -> u64 {
        self.source_attempts.saturating_add(self.repair_attempts)
    }

    fn total_would_block(&self) -> u64 {
        self.source_would_block
            .saturating_add(self.repair_would_block)
    }

    fn total_closed(&self) -> u64 {
        self.source_closed.saturating_add(self.repair_closed)
    }

    fn tree_summary(&self) -> String {
        self.per_tree
            .iter()
            .map(|(tree_id, stats)| {
                format!(
                    "{}:sa={},sq={},sw={},sc={},ra={},rq={},rw={},rc={}",
                    tree_id,
                    stats.source_attempts,
                    stats.source_queued,
                    stats.source_would_block,
                    stats.source_closed,
                    stats.repair_attempts,
                    stats.repair_queued,
                    stats.repair_would_block,
                    stats.repair_closed
                )
            })
            .collect::<Vec<_>>()
            .join(";")
    }
}

impl FecSender {
    /// Build the initial FEC sender state for a validated manifest.
    pub(super) fn new(
        manifest: &LosslessSessionManifest,
        plan: BlockPlan,
    ) -> Result<Self, &'static str> {
        let LosslessSessionMode::Fec(fec) = &manifest.mode else {
            return Err("attempted to build fec sender for plain manifest");
        };
        let scheme = fec
            .scheme_kind()
            .ok_or("unsupported fec scheme for fec sender")?;
        let geometry = plan
            .symbol_geometry(fec.symbols_per_block)
            .map_err(|_| "invalid symbol geometry for fec sender")?;
        let source_symbols = usize::try_from(fec.symbols_per_block)
            .map_err(|_| "symbols_per_block does not fit this host")?;
        let initial_symbol_count = session_fec::initial_symbol_count(BlockParams::with_scheme(
            source_symbols,
            geometry.symbol_size(),
            0,
            scheme,
        ))
        .ok_or("invalid initial fec symbol count")?;
        let block_count =
            usize::try_from(plan.total_blocks()).map_err(|_| "too many blocks for fec sender")?;

        Ok(Self {
            blocks: (0..block_count)
                .map(|_| FecBlockState {
                    next_source_symbol: 0,
                    next_fountain_symbol: initial_symbol_count,
                    required_extra_symbols: 0,
                    emitted_extra_symbols: 0,
                    encoder: None,
                    mettle_stream: None,
                })
                .collect(),
            scheme,
            symbols_per_block: fec.symbols_per_block,
            initial_symbol_count,
            tree_ids: fec.tree_ids.clone(),
            geometry,
            next_tree_rr: 0,
            current_source_cache: None,
            frame_scratch: Vec::new(),
            round_source_done_sent: false,
            current_round_id: 0,
            phase: RoundPhase::SendingData,
            round_complete: false,
            round_reports: BTreeMap::new(),
            repair_window_symbols: 0,
            protocol_error: false,
            stats: FecSenderStats::new(&fec.tree_ids),
        })
    }

    /// Main send loop for FEC mode.
    ///
    /// Source symbols are always sent before extra fountain symbols. The sender
    /// starts repair as soon as the first useful Need snapshot arrives, but it
    /// does not open the next feedback round until the current one is fully
    /// drained and every frozen quorum peer has reported.
    pub(super) async fn run(
        &mut self,
        shared: &mut super::SenderShared,
        ctrl_rx: &mut mpsc::Receiver<InboundFrame>,
    ) -> SessionOutcome {
        while !self.is_complete() {
            shared.drain_controls(ctrl_rx, self);
            if self.protocol_error {
                self.log_tree_stats(shared, "protocol_error");
                return SessionOutcome::Aborted;
            }

            if let Some((block_id, symbol_id)) = self.next_source_symbol(shared) {
                if self.send_source_symbol(shared, block_id, symbol_id).await {
                    self.round_source_done_sent = false;
                    continue;
                }
                self.log_tree_stats(shared, "source_send_wait");
                if !shared.wait_for_signal(ctrl_rx, self).await {
                    self.log_tree_stats(shared, "source_wait_aborted");
                    return SessionOutcome::Aborted;
                }
                continue;
            }

            if let Some((block_id, symbol_id)) = self.next_extra_symbol(shared) {
                if self.send_extra_symbol(shared, block_id, symbol_id).await {
                    continue;
                }
                self.log_tree_stats(shared, "repair_send_wait");
                if !shared.wait_for_signal(ctrl_rx, self).await {
                    self.log_tree_stats(shared, "repair_wait_aborted");
                    return SessionOutcome::Aborted;
                }
                continue;
            }

            if self.has_pending_repair_work()
                && let Some((block_id, symbol_id)) = self.next_extra_symbol(shared)
            {
                if self.send_extra_symbol(shared, block_id, symbol_id).await {
                    continue;
                }
                self.log_tree_stats(shared, "pending_repair_send_wait");
                if !shared.wait_for_signal(ctrl_rx, self).await {
                    self.log_tree_stats(shared, "pending_repair_wait_aborted");
                    return SessionOutcome::Aborted;
                }
                continue;
            }

            if !self.round_source_done_sent {
                self.begin_report_round(shared).await;
                self.round_source_done_sent = true;
                if shared.active_quorum_is_empty() {
                    self.round_complete = true;
                    break;
                }
                shared.start_quorum_feedback_wait();
                continue;
            }

            if self.phase == RoundPhase::WaitingForReports
                && self.round_reports.len() == shared.active_quorum.active_members().len()
            {
                self.finish_report_round(shared);
                continue;
            }

            match shared
                .wait_for_quorum_feedback(ctrl_rx, self, self.current_round_id)
                .await
            {
                super::QuorumWaitOutcome::Control | super::QuorumWaitOutcome::Solicited => {}
                super::QuorumWaitOutcome::TimedOut | super::QuorumWaitOutcome::Closed => {
                    self.log_tree_stats(shared, "quorum_wait_aborted");
                    return SessionOutcome::Aborted;
                }
            }
        }

        self.log_tree_stats(shared, "run_complete");
        SessionOutcome::Completed
    }

    pub(super) fn is_complete(&self) -> bool {
        self.round_complete
    }

    /// Return the next source symbol to send in FEC mode.
    fn next_source_symbol(&mut self, _shared: &super::SenderShared) -> Option<(u64, u32)> {
        for (block_idx, block) in self.blocks.iter_mut().enumerate() {
            let block_id = block_idx as u64;
            if block.next_source_symbol < self.initial_symbol_count
                && self.phase == RoundPhase::SendingData
            {
                return Some((block_id, block.next_source_symbol));
            }
        }
        None
    }

    /// Return the next extra fountain symbol requested by a receiver.
    fn next_extra_symbol(&mut self, _shared: &super::SenderShared) -> Option<(u64, u32)> {
        if self.total_emitted_extra_symbols() >= self.repair_window_symbols {
            return None;
        }
        for (block_idx, block) in self.blocks.iter_mut().enumerate() {
            let block_id = block_idx as u64;
            if block.emitted_extra_symbols < block.required_extra_symbols {
                return Some((block_id, block.next_fountain_symbol));
            }
        }
        None
    }

    /// Encode and send one source symbol in FEC mode.
    async fn send_source_symbol(
        &mut self,
        shared: &mut super::SenderShared,
        block_id: u64,
        symbol_id: u32,
    ) -> bool {
        let payload_started = Instant::now();
        let Some(payload) = self.source_symbol_payload(shared, block_id, symbol_id) else {
            self.stats
                .record_payload_build(SymbolKind::Source, payload_started.elapsed());
            return false;
        };
        self.stats
            .record_payload_build(SymbolKind::Source, payload_started.elapsed());
        shared.pace(payload.len()).await;
        if !self.try_send_symbol(
            shared,
            block_id,
            symbol_id,
            payload.as_ref(),
            SymbolKind::Source,
        ) {
            return false;
        }

        if let Some(block) = fec_block_mut(self, block_id) {
            block.next_source_symbol += 1;
        }
        self.discard_sent_mettle_symbol(block_id, symbol_id);
        shared.mark_payload_emitted();
        true
    }

    /// Encode and send one extra fountain symbol in FEC mode.
    async fn send_extra_symbol(
        &mut self,
        shared: &mut super::SenderShared,
        block_id: u64,
        symbol_id: u32,
    ) -> bool {
        let payload_started = Instant::now();
        let Some(payload) = self.extra_symbol_payload(shared, block_id, symbol_id) else {
            self.stats
                .record_payload_build(SymbolKind::Repair, payload_started.elapsed());
            self.protocol_error = true;
            return false;
        };
        self.stats
            .record_payload_build(SymbolKind::Repair, payload_started.elapsed());
        shared.pace(payload.len()).await;
        if !self.try_send_symbol(shared, block_id, symbol_id, &payload, SymbolKind::Repair) {
            return false;
        }

        if let Some(block) = fec_block_mut(self, block_id) {
            block.emitted_extra_symbols = block.emitted_extra_symbols.saturating_add(1);
            block.next_fountain_symbol += 1;
        }
        self.discard_sent_mettle_symbol(block_id, symbol_id);
        shared.mark_payload_emitted();
        true
    }

    /// Try to emit one FEC symbol on the first tree that currently accepts it.
    fn try_send_symbol(
        &mut self,
        shared: &mut super::SenderShared,
        block_id: u64,
        symbol_id: u32,
        payload: &[u8],
        kind: SymbolKind,
    ) -> bool {
        if self.tree_ids.is_empty() {
            self.stats.record_stall(kind);
            self.maybe_log_progress(shared);
            return false;
        }

        let tree_count = self.tree_ids.len();
        let start_idx = self.next_tree_rr;
        let initial_tree_id = self.tree_ids[start_idx];
        block_symbol_frame::encode_into(
            &mut self.frame_scratch,
            shared.session.session_id,
            block_id,
            symbol_id,
            initial_tree_id,
            payload,
        );

        for offset in 0..tree_count {
            let idx = (start_idx + offset) % tree_count;
            let tree_id = self.tree_ids[idx];
            if offset > 0 {
                block_symbol_frame::patch_tree_id(&mut self.frame_scratch, tree_id)
                    .expect("encoded block symbol should accept tree-id patch");
            }
            self.stats.record_attempt(kind, tree_id);
            let submission = control::try_send_frame(
                &shared.processors,
                control::FrameRoute {
                    session_id: shared.session.session_id,
                    tree_id: Some(tree_id),
                    src_ip: shared.route.src_ip,
                    src_port: shared.route.src_port,
                    dst_ip: shared.route.dst_ip,
                    dst_port: shared.route.dst_port,
                },
                &self.frame_scratch,
            );
            self.stats.record_outcome(kind, tree_id, submission.outcome);
            match submission.outcome {
                SendOutcome::Queued => {
                    if self.stats.total_queued() == 1 {
                        info!(
                            session_id = shared.session.session_id,
                            tree_id,
                            block_id,
                            symbol_id,
                            "Lossless sender queued first FEC payload symbol"
                        );
                    }
                    self.next_tree_rr = (idx + 1) % tree_count;
                    self.maybe_log_progress(shared);
                    return true;
                }
                SendOutcome::WouldBlock => {}
                SendOutcome::Closed => {
                    warn!(
                        session_id = shared.session.session_id,
                        tree_id,
                        "Lossless sender observed closed processor ingress while sending FEC symbol"
                    );
                }
            }
        }

        self.stats.record_stall(kind);
        self.maybe_log_progress(shared);
        false
    }

    /// Return the next initial data-phase symbol payload for one block.
    ///
    /// RaptorQ sends raw source symbols here; METTLE sends paper-native coded
    /// bins whose ids happen to be in the initial departure prefix.
    fn source_symbol_payload(
        &mut self,
        shared: &super::SenderShared,
        block_id: u64,
        symbol_id: u32,
    ) -> Option<Bytes> {
        if self.scheme == FecScheme::Mettle {
            return self
                .extra_symbol_payload(shared, block_id, symbol_id)
                .map(Bytes::from);
        }

        ensure_source_symbol_cache(&shared.source, shared.plan, self, block_id)?;
        let (_, symbols) = self.current_source_cache.as_ref()?;
        let idx = usize::try_from(symbol_id).ok()?;
        symbols.get(idx).cloned()
    }

    /// Lazily build an encoder and derive one coded/fountain symbol payload.
    fn extra_symbol_payload(
        &mut self,
        shared: &super::SenderShared,
        block_id: u64,
        symbol_id: u32,
    ) -> Option<Vec<u8>> {
        if self.scheme == FecScheme::Mettle {
            return self.mettle_symbol_payload(shared, block_id, symbol_id);
        }

        let need_encoder = fec_block_ref(self, block_id).map(|block| block.encoder.is_none())?;

        if need_encoder {
            let span = shared.plan.block_span(block_id)?;
            let source_block = shared.source.padded_symbol_bytes(span, self.geometry);
            let params = BlockParams::with_scheme(
                usize::try_from(self.symbols_per_block).ok()?,
                self.geometry.symbol_size(),
                session_fec::block_seed(shared.session.session_id, block_id),
                self.scheme,
            );
            let block = fec_block_mut(self, block_id)?;
            if block.encoder.is_none() {
                let encoder_started = Instant::now();
                block.encoder = Encoder::from_block(params, source_block.as_ref());
                self.stats
                    .record_raptorq_encoder_build(encoder_started.elapsed());
            }
        }

        let symbol_started = Instant::now();
        let payload = fec_block_ref(self, block_id)
            .and_then(|block| block.encoder.as_ref())
            .and_then(|encoder| encoder.coded_symbol(symbol_id));
        self.stats
            .record_raptorq_coded_symbol(symbol_started.elapsed());
        payload
    }

    fn mettle_symbol_payload(
        &mut self,
        shared: &super::SenderShared,
        block_id: u64,
        symbol_id: u32,
    ) -> Option<Vec<u8>> {
        let needs_stream =
            fec_block_ref(self, block_id).map(|block| block.mettle_stream.is_none())?;
        if needs_stream {
            let span = shared.plan.block_span(block_id)?;
            let stream = MettleSymbolStream::new(
                span,
                self.geometry,
                session_fec::block_seed(shared.session.session_id, block_id),
                u64::from(self.symbols_per_block),
            )?;
            let block = fec_block_mut(self, block_id)?;
            if block.mettle_stream.is_none() {
                block.mettle_stream = Some(stream);
            }
        }

        let request_started = Instant::now();
        let payload = fec_block_mut(self, block_id)
            .and_then(|block| block.mettle_stream.as_mut())
            .and_then(|stream| stream.symbol_payload(&shared.source, symbol_id));
        self.stats
            .record_mettle_symbol_request(request_started.elapsed());
        payload
    }

    fn discard_sent_mettle_symbol(&mut self, block_id: u64, symbol_id: u32) {
        if self.scheme != FecScheme::Mettle {
            return;
        }
        if let Some(stream) =
            fec_block_mut(self, block_id).and_then(|block| block.mettle_stream.as_mut())
        {
            stream.discard_through(symbol_id);
        }
    }

    async fn begin_report_round(&mut self, shared: &mut super::SenderShared) {
        if self.phase == RoundPhase::WaitingForReports {
            return;
        }
        shared.send_source_done(self.current_round_id).await;
        self.round_reports.clear();
        self.phase = RoundPhase::WaitingForReports;
        self.log_tree_stats(shared, "source_done");
    }

    fn finish_report_round(&mut self, shared: &mut super::SenderShared) {
        let mut all_complete = true;

        for status in self.round_reports.values() {
            match status {
                NeedReport::Complete => {}
                NeedReport::Fec { blocks } => {
                    all_complete = false;
                    for block in blocks {
                        if let Some(entry) = self
                            .blocks
                            .get_mut(usize::try_from(block.block_id).ok().unwrap_or(usize::MAX))
                        {
                            entry.required_extra_symbols =
                                entry.required_extra_symbols.max(block.deficit_symbols);
                        }
                    }
                }
                NeedReport::Plain { .. } => {
                    self.protocol_error = true;
                    return;
                }
            }
        }

        if all_complete {
            self.round_complete = true;
            self.log_tree_stats(shared, "all_complete");
            return;
        }

        if self.has_pending_repair_work() {
            return;
        }

        self.round_reports.clear();
        self.repair_window_symbols = 0;
        shared.clear_quorum_feedback_wait();
        self.phase = RoundPhase::SendingData;
        self.current_round_id = self.current_round_id.saturating_add(1);
        self.round_source_done_sent = false;
        for block in &mut self.blocks {
            block.required_extra_symbols = 0;
            block.emitted_extra_symbols = 0;
        }
    }
}

impl super::ModeHooks for FecSender {
    fn on_need(
        &mut self,
        shared: &mut super::SenderShared,
        peer_id: usize,
        round_id: u32,
        report: NeedReport,
    ) {
        if self.phase != RoundPhase::WaitingForReports {
            debug!(
                session_id = shared.session.session_id,
                peer_id,
                round_id,
                current_round_id = self.current_round_id,
                phase = ?self.phase,
                "Lossless FEC sender dropped Need because no feedback round is open"
            );
            return;
        }
        if round_id != self.current_round_id {
            debug!(
                session_id = shared.session.session_id,
                peer_id,
                round_id,
                current_round_id = self.current_round_id,
                "Lossless FEC sender dropped stale or future Need"
            );
            return;
        }
        if let Some(existing) = self.round_reports.get(&peer_id) {
            if existing != &report {
                warn!(
                    session_id = shared.session.session_id,
                    peer_id,
                    round_id,
                    "Lossless FEC sender rejected changed same-round Need from a quorum peer"
                );
                self.protocol_error = true;
            }
            return;
        }
        self.round_reports.insert(peer_id, report.clone());
        shared
            .quorum_liveness
            .note_feedback_progress(tokio::time::Instant::now());
        match report {
            NeedReport::Complete => {}
            NeedReport::Fec { blocks } => {
                for block in blocks {
                    if let Some(entry) = self
                        .blocks
                        .get_mut(usize::try_from(block.block_id).ok().unwrap_or(usize::MAX))
                    {
                        entry.required_extra_symbols =
                            entry.required_extra_symbols.max(block.deficit_symbols);
                    }
                }
            }
            NeedReport::Plain { .. } => {
                warn!(
                    session_id = shared.session.session_id,
                    peer_id,
                    round_id,
                    "Lossless FEC sender rejected Need with mismatched report mode"
                );
                self.protocol_error = true;
                return;
            }
        }
        self.recalculate_repair_window(shared);
        if self.round_reports.len() == shared.active_quorum.active_members().len()
            && !self.has_pending_repair_work()
        {
            self.finish_report_round(shared);
        }
    }

    fn pending_feedback_peers(&self, shared: &super::SenderShared) -> Vec<usize> {
        shared
            .active_quorum
            .active_members()
            .iter()
            .copied()
            .filter(|peer_id| !self.round_reports.contains_key(peer_id))
            .collect()
    }
}

impl FecSender {
    fn has_pending_repair_work(&self) -> bool {
        self.blocks
            .iter()
            .any(|block| block.emitted_extra_symbols < block.required_extra_symbols)
    }

    fn total_required_extra_symbols(&self) -> u32 {
        self.blocks
            .iter()
            .map(|block| u32::from(block.required_extra_symbols))
            .sum()
    }

    fn total_emitted_extra_symbols(&self) -> u32 {
        self.blocks
            .iter()
            .map(|block| u32::from(block.emitted_extra_symbols))
            .sum()
    }

    fn recalculate_repair_window(&mut self, shared: &super::SenderShared) {
        let total_required = self.total_required_extra_symbols();
        if total_required == 0 {
            self.repair_window_symbols = 0;
            return;
        }

        let report_count = u32::try_from(self.round_reports.len()).unwrap_or(u32::MAX);
        let quorum_size =
            u32::try_from(shared.active_quorum.active_members().len()).unwrap_or(u32::MAX);
        if report_count == 0 || quorum_size == 0 {
            self.repair_window_symbols = 0;
            return;
        }

        let speculative_window = total_required
            .saturating_mul(report_count)
            .div_ceil(quorum_size)
            .max(1);
        self.repair_window_symbols = speculative_window.min(total_required);
    }

    fn maybe_log_progress(&mut self, shared: &super::SenderShared) {
        let now = Instant::now();
        let total_queued = self.stats.total_queued();
        let total_attempts = self.stats.total_attempts();
        let total_would_block = self.stats.total_would_block();
        let queued_delta = total_queued.saturating_sub(self.stats.last_progress_queued);
        let attempts_delta = total_attempts.saturating_sub(self.stats.last_progress_attempts);
        let would_block_delta =
            total_would_block.saturating_sub(self.stats.last_progress_would_block);
        if attempts_delta == 0 {
            return;
        }
        let interval_due =
            now.duration_since(self.stats.last_progress_at) >= FEC_SENDER_PROGRESS_LOG_INTERVAL;
        let symbol_due = queued_delta >= FEC_SENDER_PROGRESS_SYMBOL_INTERVAL;
        if !interval_due && !symbol_due {
            return;
        }

        info!(
            session_id = shared.session.session_id,
            round_id = self.current_round_id,
            phase = ?self.phase,
            source_queued = self.stats.source_queued,
            source_would_block = self.stats.source_would_block,
            repair_queued = self.stats.repair_queued,
            repair_would_block = self.stats.repair_would_block,
            source_send_stalls = self.stats.source_send_stalls,
            repair_send_stalls = self.stats.repair_send_stalls,
            source_payload_builds = self.stats.source_payload_builds,
            source_payload_build_nanos =
                saturating_u128_to_u64(self.stats.source_payload_build_nanos),
            source_payload_avg_nanos = average_nanos(
                self.stats.source_payload_build_nanos,
                self.stats.source_payload_builds,
            ),
            repair_payload_builds = self.stats.repair_payload_builds,
            repair_payload_build_nanos =
                saturating_u128_to_u64(self.stats.repair_payload_build_nanos),
            repair_payload_avg_nanos = average_nanos(
                self.stats.repair_payload_build_nanos,
                self.stats.repair_payload_builds,
            ),
            queued_delta,
            attempts_delta,
            would_block_delta,
            repair_window_symbols = self.repair_window_symbols,
            total_required_extra_symbols = self.total_required_extra_symbols(),
            total_emitted_extra_symbols = self.total_emitted_extra_symbols(),
            "Lossless FEC sender progress"
        );

        self.stats.last_progress_at = now;
        self.stats.last_progress_queued = total_queued;
        self.stats.last_progress_attempts = total_attempts;
        self.stats.last_progress_would_block = total_would_block;
    }

    fn log_tree_stats(&self, shared: &super::SenderShared, reason: &'static str) {
        info!(
            session_id = shared.session.session_id,
            round_id = self.current_round_id,
            phase = ?self.phase,
            reason,
            source_attempts = self.stats.source_attempts,
            source_queued = self.stats.source_queued,
            source_would_block = self.stats.source_would_block,
            source_closed = self.stats.source_closed,
            repair_attempts = self.stats.repair_attempts,
            repair_queued = self.stats.repair_queued,
            repair_would_block = self.stats.repair_would_block,
            repair_closed = self.stats.repair_closed,
            closed_symbols = self.stats.total_closed(),
            source_send_stalls = self.stats.source_send_stalls,
            repair_send_stalls = self.stats.repair_send_stalls,
            source_payload_builds = self.stats.source_payload_builds,
            source_payload_build_nanos =
                saturating_u128_to_u64(self.stats.source_payload_build_nanos),
            source_payload_avg_nanos = average_nanos(
                self.stats.source_payload_build_nanos,
                self.stats.source_payload_builds,
            ),
            repair_payload_builds = self.stats.repair_payload_builds,
            repair_payload_build_nanos =
                saturating_u128_to_u64(self.stats.repair_payload_build_nanos),
            repair_payload_avg_nanos = average_nanos(
                self.stats.repair_payload_build_nanos,
                self.stats.repair_payload_builds,
            ),
            raptorq_encoder_builds = self.stats.raptorq_encoder_builds,
            raptorq_encoder_build_nanos =
                saturating_u128_to_u64(self.stats.raptorq_encoder_build_nanos),
            raptorq_encoder_build_avg_nanos = average_nanos(
                self.stats.raptorq_encoder_build_nanos,
                self.stats.raptorq_encoder_builds,
            ),
            raptorq_coded_symbol_builds = self.stats.raptorq_coded_symbol_builds,
            raptorq_coded_symbol_nanos =
                saturating_u128_to_u64(self.stats.raptorq_coded_symbol_nanos),
            raptorq_coded_symbol_avg_nanos = average_nanos(
                self.stats.raptorq_coded_symbol_nanos,
                self.stats.raptorq_coded_symbol_builds,
            ),
            mettle_symbol_requests = self.stats.mettle_symbol_requests,
            mettle_symbol_request_nanos =
                saturating_u128_to_u64(self.stats.mettle_symbol_request_nanos),
            mettle_symbol_request_avg_nanos = average_nanos(
                self.stats.mettle_symbol_request_nanos,
                self.stats.mettle_symbol_requests,
            ),
            mettle_streams = %self.mettle_stream_summary(),
            repair_window_symbols = self.repair_window_symbols,
            total_required_extra_symbols = self.total_required_extra_symbols(),
            total_emitted_extra_symbols = self.total_emitted_extra_symbols(),
            tree_stats = %self.stats.tree_summary(),
            "Lossless FEC sender per-tree stats"
        );
    }

    fn mettle_stream_summary(&self) -> String {
        self.blocks
            .iter()
            .enumerate()
            .filter_map(|(block_id, block)| {
                let stream = block.mettle_stream.as_ref()?;
                Some(format!(
                    "{}:next_src={},buf={},pushed={},bins={},adv={},fin={}",
                    block_id,
                    stream.next_source_id,
                    stream.buffered_bins.len(),
                    stream.source_symbols_pushed,
                    stream.bins_buffered_total,
                    stream.advance_calls,
                    stream.finish_calls
                ))
            })
            .collect::<Vec<_>>()
            .join(";")
    }
}

/// Refresh the cached source-symbol slice for `block_id` if needed.
fn ensure_source_symbol_cache(
    source: &super::BlockSource,
    plan: BlockPlan,
    fec: &mut FecSender,
    block_id: u64,
) -> Option<()> {
    let needs_refresh = !matches!(
        &fec.current_source_cache,
        Some((cached_block_id, _)) if *cached_block_id == block_id
    );
    if !needs_refresh {
        return Some(());
    }

    let span = plan.block_span(block_id)?;
    fec.current_source_cache = Some((block_id, source.source_symbols(span, fec.geometry)));
    Some(())
}

fn saturating_u128_to_u64(value: u128) -> u64 {
    u64::try_from(value).unwrap_or(u64::MAX)
}

fn average_nanos(total_nanos: u128, count: u64) -> u64 {
    if count == 0 {
        return 0;
    }
    saturating_u128_to_u64(total_nanos / u128::from(count))
}

/// Borrow mutable FEC state for one block.
fn fec_block_mut(fec: &mut FecSender, block_id: u64) -> Option<&mut FecBlockState> {
    let idx = usize::try_from(block_id).ok()?;
    fec.blocks.get_mut(idx)
}

/// Borrow immutable FEC state for one block.
fn fec_block_ref(fec: &FecSender, block_id: u64) -> Option<&FecBlockState> {
    let idx = usize::try_from(block_id).ok()?;
    fec.blocks.get(idx)
}

#[cfg(test)]
mod tests {
    use std::net::Ipv4Addr;
    use std::time::Duration;

    use bytes::Bytes;

    use super::*;
    use crate::node::config::LocalConfig;
    use crate::node::processor::ProcessorHandle;
    use crate::node::session::plan::BlockPlan;
    use crate::node::session::runtime::{SessionConfig, TransportRoute};
    use crate::node::session::sender::state::{ActiveSessionQuorum, QuorumLiveness};
    use crate::node::session::sender::{BlockSource, ModeHooks, SenderShared};
    use nextmini_messages::lossless_session::{LosslessSessionFecMode, NeedBlock};

    #[tokio::test]
    async fn fec_sender_drops_future_round_need() {
        let manifest = test_manifest();
        let plan = BlockPlan::new(16, 16).expect("valid plan");
        let mut sender = FecSender::new(&manifest, plan).expect("sender should build");
        sender.phase = RoundPhase::WaitingForReports;
        sender.current_round_id = 0;
        let mut shared = test_sender_shared(manifest.clone());

        sender.on_need(
            &mut shared,
            22,
            1,
            NeedReport::Fec {
                blocks: vec![NeedBlock {
                    block_id: 0,
                    deficit_symbols: 1,
                }],
            },
        );

        assert!(sender.round_reports.is_empty());
        assert!(!sender.has_pending_repair_work());
        assert!(!sender.protocol_error);
    }

    #[tokio::test]
    async fn fec_sender_drops_need_after_round_closure() {
        let manifest = test_manifest();
        let plan = BlockPlan::new(16, 16).expect("valid plan");
        let mut sender = FecSender::new(&manifest, plan).expect("sender should build");
        sender.phase = RoundPhase::SendingData;
        sender.current_round_id = 0;
        let mut shared = test_sender_shared(manifest.clone());

        sender.on_need(
            &mut shared,
            22,
            0,
            NeedReport::Fec {
                blocks: vec![NeedBlock {
                    block_id: 0,
                    deficit_symbols: 1,
                }],
            },
        );

        assert!(sender.round_reports.is_empty());
        assert!(!sender.has_pending_repair_work());
        assert!(!sender.protocol_error);
    }

    #[tokio::test]
    async fn fec_sender_bounds_speculative_repair_until_more_quorum_reports_arrive() {
        let manifest = test_manifest();
        let plan = BlockPlan::new(16, 16).expect("valid plan");
        let mut sender = FecSender::new(&manifest, plan).expect("sender should build");
        sender.phase = RoundPhase::WaitingForReports;
        sender.current_round_id = 0;
        let mut shared = test_sender_shared(manifest.clone());
        shared.active_quorum = ActiveSessionQuorum::new([22, 23]);
        shared.active_quorum.record_ready(22);
        shared.active_quorum.record_ready(23);
        shared.active_quorum.freeze();

        sender.on_need(
            &mut shared,
            22,
            0,
            NeedReport::Fec {
                blocks: vec![NeedBlock {
                    block_id: 0,
                    deficit_symbols: 4,
                }],
            },
        );

        assert!(sender.has_pending_repair_work());
        assert_eq!(
            sender.next_extra_symbol(&shared),
            Some((0, sender.symbols_per_block)),
            "first quorum report should open a bounded speculative repair window"
        );
        if let Some(block) = sender.blocks.first_mut() {
            block.emitted_extra_symbols = 2;
            block.next_fountain_symbol = sender.symbols_per_block + 2;
        }
        assert_eq!(
            sender.next_extra_symbol(&shared),
            None,
            "speculative repair should stop once the partial window is exhausted"
        );

        sender.on_need(&mut shared, 23, 0, NeedReport::Complete);

        assert_eq!(
            sender.next_extra_symbol(&shared),
            Some((0, sender.symbols_per_block + 2)),
            "full quorum should open the remaining repair budget"
        );
    }

    #[test]
    fn mettle_sender_accepts_small_experimental_k() {
        let manifest = LosslessSessionManifest {
            block_size: 16,
            total_bytes: 16,
            total_blocks: 1,
            mode: LosslessSessionMode::Fec(LosslessSessionFecMode::new_mettle(16, vec![7])),
        };
        let plan = BlockPlan::new(16, 16).expect("valid plan");

        assert!(FecSender::new(&manifest, plan).is_ok());
    }

    #[test]
    fn mettle_sender_accepts_large_stream_scale_k() {
        let k = 131_072u32;
        let block_size = 1_073_741_824u32;
        let manifest = LosslessSessionManifest {
            block_size,
            total_bytes: u64::from(block_size),
            total_blocks: 1,
            mode: LosslessSessionMode::Fec(LosslessSessionFecMode::new_mettle(k, vec![7])),
        };
        let plan = BlockPlan::new(u64::from(block_size), block_size as usize).expect("valid plan");

        let sender = FecSender::new(&manifest, plan).expect("large-K METTLE sender");

        assert_eq!(sender.symbols_per_block, k);
        assert!(
            sender.initial_symbol_count > k,
            "METTLE initial phase should emit the finalized coded-bin prefix"
        );
    }

    #[tokio::test]
    async fn mettle_sender_streams_large_k_symbols_on_demand() {
        let k = 131_072u32;
        let block_size = 1_073_741_824u32;
        let manifest = LosslessSessionManifest {
            block_size,
            total_bytes: u64::from(block_size),
            total_blocks: 1,
            mode: LosslessSessionMode::Fec(LosslessSessionFecMode::new_mettle(k, vec![7])),
        };
        let plan = BlockPlan::new(u64::from(block_size), block_size as usize).expect("valid plan");
        let mut sender = FecSender::new(&manifest, plan).expect("large-K METTLE sender");
        let mut source = vec![0u8; 16 * 1024];
        source[..8192].fill(0xA5);
        source[8192..].fill(0x5A);
        let shared = test_sender_shared_with_source(manifest, Bytes::from(source));

        let first = sender
            .source_symbol_payload(&shared, 0, 0)
            .expect("first METTLE bin");

        assert_eq!(first.len(), 8192);
        let stream = sender.blocks[0]
            .mettle_stream
            .as_ref()
            .expect("stream encoder should be initialized lazily");
        assert_eq!(
            stream.next_source_id, 1,
            "requesting bin 0 should not scan or materialize the whole block"
        );
        assert_eq!(stream.buffered_bin_count(), 1);

        sender.discard_sent_mettle_symbol(0, 0);
        assert_eq!(
            sender.blocks[0]
                .mettle_stream
                .as_ref()
                .expect("stream still present")
                .buffered_bin_count(),
            0
        );
    }

    fn test_manifest() -> LosslessSessionManifest {
        LosslessSessionManifest {
            block_size: 16,
            total_bytes: 16,
            total_blocks: 1,
            mode: LosslessSessionMode::Fec(LosslessSessionFecMode::new_raptorq(4, vec![7, 9])),
        }
    }

    fn test_sender_shared(manifest: LosslessSessionManifest) -> SenderShared {
        test_sender_shared_with_source(manifest, Bytes::from_static(b"abcdefghijklmnop"))
    }

    fn test_sender_shared_with_source(
        manifest: LosslessSessionManifest,
        source_buffer: Bytes,
    ) -> SenderShared {
        let processors = ProcessorHandle::new(LocalConfig {
            node_id: 0,
            n_nodes: 1,
            num_packet_processors: 1,
            channel_capacity: 8,
            user_space_base_addr: Ipv4Addr::new(10, 0, 0, 0),
            local_netmask: Ipv4Addr::new(255, 255, 255, 0),
            ..Default::default()
        });
        let block_size = usize::try_from(manifest.block_size).expect("test block size fits usize");
        let plan = BlockPlan::new(manifest.total_bytes, block_size).expect("valid plan");

        SenderShared {
            session: SessionConfig {
                session_id: 7,
                block_size,
            },
            route: TransportRoute {
                src_ip: Ipv4Addr::new(10, 0, 0, 1),
                dst_ip: Ipv4Addr::new(10, 0, 0, 2),
                src_port: 1111,
                dst_port: 2222,
            },
            processors,
            manifest,
            receiver_ids: vec![22],
            active_quorum: ActiveSessionQuorum::new([22]),
            quorum_liveness: QuorumLiveness::new(
                Duration::from_millis(10),
                Duration::from_millis(30),
            ),
            plan,
            source: BlockSource::new(source_buffer.clone(), source_buffer.len() as u64),
            ready_grace: Duration::from_millis(1),
            topology_ready: None,
            pacer: None,
            payload_emitted: false,
            cloudcast_tree_ids: None,
        }
    }
}
