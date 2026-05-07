use bytes::Bytes;
use std::collections::BTreeMap;
use std::num::NonZeroUsize;
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

const WEIGHTED_TREE_SCHEDULE_SLOTS: usize = 256;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct TreeScheduleEntry {
    tree_id: u16,
    tree_index: usize,
}

/// FEC-mode sender state and scheduling cursors.
pub(super) struct FecSender {
    blocks: Vec<FecBlockState>,
    scheme: FecScheme,
    symbols_per_block: u32,
    initial_symbol_count: u32,
    mettle_stream_symbol_limit: u32,
    mettle_overhead: mettle::OverheadRatio,
    tree_ids: Vec<u16>,
    tree_schedule: Vec<TreeScheduleEntry>,
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

/// Per-block finite-stream METTLE encoder state.
///
/// It keeps only the paper encoder's rolling coupling window plus any
/// finalized bins emitted ahead of the currently retried symbol.
struct MettleSymbolStream {
    encoder: Option<mettle::stream::Encoder>,
    span: BlockSpan,
    geometry: SymbolGeometry,
    real_source_count: u64,
    next_source_id: u64,
    finished: bool,
    buffered_bins: BTreeMap<u32, Vec<u8>>,
    advance_calls: u64,
    source_symbols_pushed: u64,
    bins_buffered_total: u64,
}

impl MettleSymbolStream {
    fn new(
        span: BlockSpan,
        geometry: SymbolGeometry,
        seed: u64,
        real_source_count: u64,
        mettle_overhead: mettle::OverheadRatio,
    ) -> Option<Self> {
        let source_symbol_bytes = NonZeroUsize::new(geometry.symbol_size())?;
        Some(Self {
            encoder: Some(mettle::stream::Encoder::new_terminated(
                mettle::MettleParams::new(mettle_overhead),
                source_symbol_bytes,
                seed,
                real_source_count,
            )),
            span,
            geometry,
            real_source_count,
            next_source_id: 0,
            finished: false,
            buffered_bins: BTreeMap::new(),
            advance_calls: 0,
            source_symbols_pushed: 0,
            bins_buffered_total: 0,
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
        let bins = if self.next_source_id < self.real_source_count {
            let source_index = usize::try_from(self.next_source_id).ok()?;
            let payload = source.source_symbol_payload(self.span, self.geometry, source_index)?;
            self.next_source_id += 1;
            self.source_symbols_pushed = self.source_symbols_pushed.saturating_add(1);
            self.encoder.as_mut()?.push_source(&payload)
        } else {
            if self.finished {
                return Some(false);
            }
            self.finished = true;
            self.encoder.take()?.finish()
        };
        self.buffer_bins(bins)?;
        Some(true)
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
}

impl FecSenderStats {
    fn new(_tree_ids: &[u16]) -> Self {
        Self {
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
        }
    }

    fn record_attempt(&mut self, kind: SymbolKind, _tree_id: u16) {
        match kind {
            SymbolKind::Source => {
                self.source_attempts = self.source_attempts.saturating_add(1);
            }
            SymbolKind::Repair => {
                self.repair_attempts = self.repair_attempts.saturating_add(1);
            }
        }
    }

    fn record_outcome(&mut self, kind: SymbolKind, _tree_id: u16, outcome: SendOutcome) {
        match (kind, outcome) {
            (SymbolKind::Source, SendOutcome::Queued) => {
                self.source_queued = self.source_queued.saturating_add(1);
            }
            (SymbolKind::Source, SendOutcome::WouldBlock) => {
                self.source_would_block = self.source_would_block.saturating_add(1);
            }
            (SymbolKind::Source, SendOutcome::Closed) => {
                self.source_closed = self.source_closed.saturating_add(1);
            }
            (SymbolKind::Repair, SendOutcome::Queued) => {
                self.repair_queued = self.repair_queued.saturating_add(1);
            }
            (SymbolKind::Repair, SendOutcome::WouldBlock) => {
                self.repair_would_block = self.repair_would_block.saturating_add(1);
            }
            (SymbolKind::Repair, SendOutcome::Closed) => {
                self.repair_closed = self.repair_closed.saturating_add(1);
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
}

fn build_tree_schedule(tree_ids: &[u16], tree_weights: &[f64]) -> Vec<TreeScheduleEntry> {
    if tree_ids.is_empty() {
        return Vec::new();
    }
    if tree_weights.len() != tree_ids.len() {
        return unweighted_tree_schedule(tree_ids);
    }

    let weights = tree_weights
        .iter()
        .map(|weight| {
            if weight.is_finite() && *weight > 0.0 {
                *weight
            } else {
                1.0
            }
        })
        .collect::<Vec<_>>();
    let total_weight = weights.iter().sum::<f64>();
    if !total_weight.is_finite() || total_weight <= 0.0 {
        return unweighted_tree_schedule(tree_ids);
    }

    let schedule_len = tree_ids.len().max(WEIGHTED_TREE_SCHEDULE_SLOTS);
    let mut slot_counts = vec![1usize; tree_ids.len()];
    let remaining_slots = schedule_len.saturating_sub(tree_ids.len());
    let mut assigned_slots = 0usize;
    let mut remainders = Vec::with_capacity(tree_ids.len());
    for (idx, weight) in weights.iter().enumerate() {
        let exact = (*weight / total_weight) * remaining_slots as f64;
        let base = exact.floor() as usize;
        slot_counts[idx] += base;
        assigned_slots += base;
        remainders.push((idx, exact - base as f64, *weight));
    }
    remainders.sort_by(|left, right| {
        right
            .1
            .total_cmp(&left.1)
            .then_with(|| right.2.total_cmp(&left.2))
            .then_with(|| left.0.cmp(&right.0))
    });
    for idx in 0..remaining_slots.saturating_sub(assigned_slots) {
        let tree_index = remainders[idx % remainders.len()].0;
        slot_counts[tree_index] += 1;
    }

    interleave_tree_slots(tree_ids, &slot_counts)
}

fn unweighted_tree_schedule(tree_ids: &[u16]) -> Vec<TreeScheduleEntry> {
    tree_ids
        .iter()
        .copied()
        .enumerate()
        .map(|(tree_index, tree_id)| TreeScheduleEntry {
            tree_id,
            tree_index,
        })
        .collect()
}

fn interleave_tree_slots(tree_ids: &[u16], slot_counts: &[usize]) -> Vec<TreeScheduleEntry> {
    let total_slots = slot_counts.iter().sum();
    let mut positioned = Vec::with_capacity(total_slots);
    for (tree_index, (&tree_id, &slot_count)) in tree_ids.iter().zip(slot_counts).enumerate() {
        for slot_idx in 0..slot_count {
            let position = (slot_idx as f64 + 0.5) / slot_count as f64;
            positioned.push((position, tree_index, tree_id));
        }
    }
    positioned.sort_by(|left, right| {
        left.0
            .total_cmp(&right.0)
            .then_with(|| left.1.cmp(&right.1))
    });
    positioned
        .into_iter()
        .map(|(_, tree_index, tree_id)| TreeScheduleEntry {
            tree_id,
            tree_index,
        })
        .collect()
}

fn mark_tree_attempted(
    tree_index: usize,
    tried_mask: &mut u128,
    tried_large: Option<&mut Vec<bool>>,
) -> bool {
    if let Some(tried) = tried_large {
        let Some(entry) = tried.get_mut(tree_index) else {
            return false;
        };
        if *entry {
            return false;
        }
        *entry = true;
        return true;
    }

    let Some(bit) = 1u128.checked_shl(tree_index as u32) else {
        return false;
    };
    if *tried_mask & bit != 0 {
        return false;
    }
    *tried_mask |= bit;
    true
}

impl FecSender {
    /// Build the initial FEC sender state for a validated manifest.
    #[cfg(test)]
    pub(super) fn new(
        manifest: &LosslessSessionManifest,
        plan: BlockPlan,
    ) -> Result<Self, &'static str> {
        Self::new_with_tree_weights(manifest, plan, &[])
    }

    /// Build FEC sender state using optional solver-derived tree weights.
    pub(super) fn new_with_tree_weights(
        manifest: &LosslessSessionManifest,
        plan: BlockPlan,
        tree_weights: &[f64],
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
        let mettle_overhead = session_fec::mettle_overhead_from_fec_mode(fec)
            .ok_or("invalid METTLE coded rate in fec sender manifest")?;
        let initial_symbol_count = session_fec::initial_symbol_count(
            BlockParams::with_scheme(source_symbols, geometry.symbol_size(), 0, scheme),
            mettle_overhead,
        )
        .ok_or("invalid initial fec symbol count")?;
        let mettle_stream_symbol_limit = if scheme == FecScheme::Mettle {
            mettle::block::BlockParams::with_overhead(
                source_symbols,
                geometry.symbol_size(),
                0,
                mettle_overhead,
            )
            .metadata()
            .ok()
            .and_then(|metadata| u32::try_from(metadata.symbol_count()).ok())
            .ok_or("invalid METTLE finite stream symbol count")?
        } else {
            0
        };
        let block_count =
            usize::try_from(plan.total_blocks()).map_err(|_| "too many blocks for fec sender")?;
        if scheme == FecScheme::Mettle && block_count > 1 {
            return Err("paper-native METTLE requires one logical object stream");
        }
        let tree_schedule = build_tree_schedule(&fec.tree_ids, tree_weights);

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
            mettle_stream_symbol_limit,
            mettle_overhead,
            tree_ids: fec.tree_ids.clone(),
            tree_schedule,
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
    /// RaptorQ sends raw source symbols before feedback-driven repairs.
    /// METTLE uses one paper-native finite object stream: the sender pushes
    /// the real object symbols into a terminated stream and then emits the
    /// encoder's finish tail. SourceDone is not used to estimate a finite
    /// repair budget for METTLE.
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
                if !self.send_source_symbol(shared, block_id, symbol_id).await {
                    self.log_tree_stats(shared, "source_send_failed");
                    return SessionOutcome::Aborted;
                }
                self.round_source_done_sent = false;
                continue;
            }

            if let Some((block_id, symbol_id)) = self.next_extra_symbol(shared) {
                if !self.send_extra_symbol(shared, block_id, symbol_id).await {
                    self.log_tree_stats(shared, "repair_send_failed");
                    return SessionOutcome::Aborted;
                }
                continue;
            }

            if self.has_pending_repair_work()
                && let Some((block_id, symbol_id)) = self.next_extra_symbol(shared)
            {
                if !self.send_extra_symbol(shared, block_id, symbol_id).await {
                    self.log_tree_stats(shared, "pending_repair_send_failed");
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
                if !self.waiting_for_mettle_late_complete() {
                    self.finish_report_round(shared);
                    continue;
                }
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
        let source_phase_limit = self.source_phase_symbol_limit();
        for (block_idx, block) in self.blocks.iter_mut().enumerate() {
            let block_id = block_idx as u64;
            if block.next_source_symbol < source_phase_limit
                && self.phase == RoundPhase::SendingData
            {
                return Some((block_id, block.next_source_symbol));
            }
        }
        None
    }

    /// Return the next extra fountain symbol requested by a receiver.
    fn next_extra_symbol(&mut self, _shared: &super::SenderShared) -> Option<(u64, u32)> {
        if self.scheme == FecScheme::Mettle {
            return None;
        }
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

    fn source_phase_symbol_limit(&self) -> u32 {
        match self.scheme {
            FecScheme::RaptorQ => self.initial_symbol_count,
            FecScheme::Mettle => self.mettle_stream_symbol_limit,
        }
    }

    /// Encode and send one source symbol in FEC mode.
    async fn send_source_symbol(
        &mut self,
        shared: &mut super::SenderShared,
        block_id: u64,
        symbol_id: u32,
    ) -> bool {
        let Some(payload) = self.source_symbol_payload(shared, block_id, symbol_id) else {
            return false;
        };
        shared.pace(payload.len()).await;
        if !self
            .send_symbol(
                shared,
                block_id,
                symbol_id,
                payload.as_ref(),
                SymbolKind::Source,
            )
            .await
        {
            return false;
        }

        if let Some(block) = fec_block_mut(self, block_id) {
            let Some(next_symbol) = block.next_source_symbol.checked_add(1) else {
                self.protocol_error = true;
                return false;
            };
            block.next_source_symbol = next_symbol;
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
        let Some(payload) = self.extra_symbol_payload(shared, block_id, symbol_id) else {
            self.protocol_error = true;
            return false;
        };
        shared.pace(payload.len()).await;
        if !self
            .send_symbol(shared, block_id, symbol_id, &payload, SymbolKind::Repair)
            .await
        {
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

    /// Emit one FEC symbol through the processor ingress path.
    async fn send_symbol(
        &mut self,
        shared: &mut super::SenderShared,
        block_id: u64,
        symbol_id: u32,
        payload: &[u8],
        kind: SymbolKind,
    ) -> bool {
        if self.tree_schedule.is_empty() {
            self.stats.record_stall(kind);
            self.maybe_log_progress(shared);
            return false;
        }

        let tree_count = self.tree_ids.len();
        let schedule_count = self.tree_schedule.len();
        let start_idx = self.next_tree_rr % schedule_count;
        let initial_tree_id = self.tree_schedule[start_idx].tree_id;
        block_symbol_frame::encode_into(
            &mut self.frame_scratch,
            shared.session.session_id,
            block_id,
            symbol_id,
            initial_tree_id,
            payload,
        );

        let mut await_idx = None;
        let mut frame_tree_id = initial_tree_id;
        let mut tried_mask = 0u128;
        let mut tried_large = if tree_count > 128 {
            Some(vec![false; tree_count])
        } else {
            None
        };
        let mut attempted_trees = 0usize;
        for offset in 0..schedule_count {
            let idx = (start_idx + offset) % schedule_count;
            let slot = self.tree_schedule[idx];
            if !mark_tree_attempted(slot.tree_index, &mut tried_mask, tried_large.as_mut()) {
                continue;
            }
            attempted_trees += 1;
            let tree_id = slot.tree_id;
            if tree_id != frame_tree_id {
                block_symbol_frame::patch_tree_id(&mut self.frame_scratch, tree_id)
                    .expect("encoded block symbol should accept tree-id patch");
                frame_tree_id = tree_id;
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
                    self.note_queued_symbol(
                        shared,
                        block_id,
                        symbol_id,
                        tree_id,
                        idx,
                        schedule_count,
                    );
                    return true;
                }
                SendOutcome::WouldBlock => {
                    await_idx.get_or_insert(idx);
                }
                SendOutcome::Closed => {
                    warn!(
                        session_id = shared.session.session_id,
                        tree_id,
                        "Lossless sender observed closed processor ingress while sending FEC symbol"
                    );
                }
            }
            if attempted_trees == tree_count {
                break;
            }
        }

        let Some(idx) = await_idx else {
            self.stats.record_stall(kind);
            self.maybe_log_progress(shared);
            return false;
        };
        let tree_id = self.tree_schedule[idx].tree_id;
        block_symbol_frame::patch_tree_id(&mut self.frame_scratch, tree_id)
            .expect("encoded block symbol should accept tree-id patch");
        self.stats.record_attempt(kind, tree_id);
        control::send_frame(
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
        )
        .await;
        self.stats
            .record_outcome(kind, tree_id, SendOutcome::Queued);
        self.note_queued_symbol(shared, block_id, symbol_id, tree_id, idx, schedule_count);
        true
    }

    fn note_queued_symbol(
        &mut self,
        shared: &super::SenderShared,
        block_id: u64,
        symbol_id: u32,
        tree_id: u16,
        idx: usize,
        schedule_count: usize,
    ) {
        if self.stats.total_queued() == 1 {
            info!(
                session_id = shared.session.session_id,
                tree_id, block_id, symbol_id, "Lossless sender queued first FEC payload symbol"
            );
        }
        self.next_tree_rr = (idx + 1) % schedule_count;
        self.maybe_log_progress(shared);
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
                block.encoder = Encoder::from_block(params, source_block.as_ref());
            }
        }

        fec_block_ref(self, block_id)
            .and_then(|block| block.encoder.as_ref())
            .and_then(|encoder| encoder.coded_symbol(symbol_id))
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
                self.mettle_overhead,
            )?;
            let block = fec_block_mut(self, block_id)?;
            if block.mettle_stream.is_none() {
                block.mettle_stream = Some(stream);
            }
        }

        fec_block_mut(self, block_id)
            .and_then(|block| block.mettle_stream.as_mut())
            .and_then(|stream| stream.symbol_payload(&shared.source, symbol_id))
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
                    if self.scheme != FecScheme::Mettle {
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

        if self.scheme == FecScheme::Mettle {
            warn!(
                session_id = shared.session.session_id,
                round_id = self.current_round_id,
                "Lossless METTLE finite stream was exhausted before every receiver decoded"
            );
            self.protocol_error = true;
            self.round_reports.clear();
            self.repair_window_symbols = 0;
            shared.clear_quorum_feedback_wait();
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
        if self.accept_mettle_early_complete(shared, peer_id, round_id, &report) {
            return;
        }
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
            if self.scheme == FecScheme::Mettle {
                match (existing, &report) {
                    (NeedReport::Complete, NeedReport::Fec { .. }) => {
                        debug!(
                            session_id = shared.session.session_id,
                            peer_id,
                            round_id,
                            "Lossless METTLE sender ignored a stale deficit after Complete"
                        );
                        return;
                    }
                    (NeedReport::Fec { .. }, NeedReport::Complete) => {
                        self.round_reports.insert(peer_id, report.clone());
                        shared
                            .quorum_liveness
                            .note_feedback_progress(tokio::time::Instant::now());
                        if self
                            .round_reports
                            .values()
                            .all(|status| matches!(status, NeedReport::Complete))
                            && self.round_reports.len()
                                == shared.active_quorum.active_members().len()
                        {
                            self.round_complete = true;
                            self.log_tree_stats(shared, "all_complete");
                        }
                        return;
                    }
                    _ => {}
                }
            }
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
                if self.scheme != FecScheme::Mettle {
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
            if self.waiting_for_mettle_late_complete() {
                return;
            }
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
    fn accept_mettle_early_complete(
        &mut self,
        shared: &mut super::SenderShared,
        peer_id: usize,
        round_id: u32,
        report: &NeedReport,
    ) -> bool {
        if self.scheme != FecScheme::Mettle
            || self.phase != RoundPhase::SendingData
            || !matches!(report, NeedReport::Complete)
        {
            return false;
        }
        if round_id > self.current_round_id {
            debug!(
                session_id = shared.session.session_id,
                peer_id,
                round_id,
                current_round_id = self.current_round_id,
                "Lossless METTLE sender dropped early Complete for a future round"
            );
            return true;
        }
        if let Some(existing) = self.round_reports.get(&peer_id) {
            if !matches!(existing, NeedReport::Complete) {
                warn!(
                    session_id = shared.session.session_id,
                    peer_id,
                    round_id,
                    "Lossless METTLE sender rejected changed early Complete from a quorum peer"
                );
                self.protocol_error = true;
            }
            return true;
        }
        self.round_reports.insert(peer_id, report.clone());
        shared
            .quorum_liveness
            .note_feedback_progress(tokio::time::Instant::now());
        if self.round_reports.len() == shared.active_quorum.active_members().len() {
            self.round_complete = true;
            self.log_tree_stats(shared, "all_complete");
        }
        true
    }

    fn has_pending_repair_work(&self) -> bool {
        self.blocks
            .iter()
            .any(|block| block.emitted_extra_symbols < block.required_extra_symbols)
    }

    fn waiting_for_mettle_late_complete(&self) -> bool {
        self.scheme == FecScheme::Mettle
            && self.phase == RoundPhase::WaitingForReports
            && self
                .round_reports
                .values()
                .any(|report| !matches!(report, NeedReport::Complete))
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

    fn maybe_log_progress(&mut self, _shared: &super::SenderShared) {
        // Intentionally empty: the per-symbol progress instrumentation is too
        // expensive for WAN throughput measurements.
    }

    fn log_tree_stats(&self, _shared: &super::SenderShared, _reason: &'static str) {
        // Intentionally empty: detailed tree stats were temporary instrumentation.
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

    #[test]
    fn fec_tree_schedule_keeps_round_robin_without_weights() {
        let schedule = build_tree_schedule(&[7, 9, 11], &[]);

        assert_eq!(
            schedule,
            vec![
                TreeScheduleEntry {
                    tree_id: 7,
                    tree_index: 0,
                },
                TreeScheduleEntry {
                    tree_id: 9,
                    tree_index: 1,
                },
                TreeScheduleEntry {
                    tree_id: 11,
                    tree_index: 2,
                },
            ]
        );
    }

    #[test]
    fn fec_tree_schedule_quantizes_solver_weights() {
        let schedule = build_tree_schedule(&[7, 9], &[3.0, 1.0]);
        let tree_7_slots = schedule.iter().filter(|entry| entry.tree_id == 7).count();
        let tree_9_slots = schedule.iter().filter(|entry| entry.tree_id == 9).count();

        assert_eq!(schedule.len(), WEIGHTED_TREE_SCHEDULE_SLOTS);
        assert!(
            tree_7_slots > tree_9_slots * 2,
            "higher solver weight should receive proportionally more send slots"
        );
        assert!(
            tree_9_slots > 0,
            "each configured tree should remain reachable as a fallback"
        );
    }

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
            mode: LosslessSessionMode::Fec(LosslessSessionFecMode::new_mettle_with_coded_rate(
                k,
                vec![7],
                21,
                20,
            )),
        };
        let plan = BlockPlan::new(u64::from(block_size), block_size as usize).expect("valid plan");

        let sender = FecSender::new(&manifest, plan).expect("large-K METTLE sender");
        let expected_initial = mettle::block::BlockParams::with_overhead(
            k as usize,
            8192,
            0,
            mettle::OverheadRatio::new(1, 20).expect("valid overhead"),
        )
        .metadata()
        .expect("large-K METTLE metadata")
        .initial_symbol_count();

        assert_eq!(sender.symbols_per_block, k);
        assert_eq!(
            sender.initial_symbol_count as usize, expected_initial,
            "METTLE keeps the configured coded-rate parameter for the stream graph"
        );
    }

    #[tokio::test]
    async fn mettle_sender_data_phase_streams_until_completion() {
        let k = 128u32;
        let block_size = 8192u32;
        let manifest = LosslessSessionManifest {
            block_size,
            total_bytes: u64::from(block_size),
            total_blocks: 1,
            mode: LosslessSessionMode::Fec(LosslessSessionFecMode::new_mettle_with_coded_rate(
                k,
                vec![7],
                21,
                20,
            )),
        };
        let plan = BlockPlan::new(u64::from(block_size), block_size as usize).expect("valid plan");
        let mut sender = FecSender::new(&manifest, plan).expect("METTLE sender");
        let metadata = mettle::block::BlockParams::with_overhead(
            k as usize,
            64,
            0,
            mettle::OverheadRatio::new(1, 20).expect("valid overhead"),
        )
        .metadata()
        .expect("METTLE metadata");
        let expected_initial =
            u32::try_from(metadata.initial_symbol_count()).expect("initial fits u32");
        let expected_symbol_count =
            u32::try_from(metadata.symbol_count()).expect("symbol count fits u32");

        assert_eq!(sender.initial_symbol_count, expected_initial);
        assert_eq!(sender.source_phase_symbol_limit(), expected_symbol_count);

        sender.blocks[0].next_source_symbol = sender.initial_symbol_count;
        let shared = test_sender_shared(manifest.clone());
        assert_eq!(
            sender.next_source_symbol(&shared),
            Some((0, sender.initial_symbol_count)),
            "METTLE should emit the paper-native finite-stream tail instead of stopping at a session window"
        );

        sender.blocks[0].next_source_symbol = sender.mettle_stream_symbol_limit;
        assert_eq!(
            sender.next_source_symbol(&shared),
            None,
            "METTLE should stop after the paper finite-stream symbol count"
        );
    }

    #[tokio::test]
    async fn mettle_sender_accepts_early_complete_while_sending_data() {
        let manifest = LosslessSessionManifest {
            block_size: 16,
            total_bytes: 16,
            total_blocks: 1,
            mode: LosslessSessionMode::Fec(LosslessSessionFecMode::new_mettle(4, vec![7])),
        };
        let plan = BlockPlan::new(16, 16).expect("valid plan");
        let mut sender = FecSender::new(&manifest, plan).expect("METTLE sender");
        let mut shared = test_sender_shared(manifest);
        shared.active_quorum.record_ready(22);
        shared.active_quorum.freeze();

        sender.on_need(&mut shared, 22, 0, NeedReport::Complete);

        assert!(sender.round_complete);
        assert_eq!(sender.round_reports.get(&22), Some(&NeedReport::Complete));
        assert!(!sender.protocol_error);
    }

    #[tokio::test]
    async fn mettle_sender_waits_for_late_complete_after_finite_stream_exhaustion() {
        let manifest = LosslessSessionManifest {
            block_size: 16,
            total_bytes: 16,
            total_blocks: 1,
            mode: LosslessSessionMode::Fec(LosslessSessionFecMode::new_mettle(4, vec![7])),
        };
        let plan = BlockPlan::new(16, 16).expect("valid plan");
        let mut sender = FecSender::new(&manifest, plan).expect("METTLE sender");
        let mut shared = test_sender_shared(manifest);
        shared.active_quorum = ActiveSessionQuorum::new([22, 23]);
        shared.active_quorum.record_ready(22);
        shared.active_quorum.record_ready(23);
        shared.active_quorum.freeze();
        shared.start_quorum_feedback_wait();
        sender.phase = RoundPhase::WaitingForReports;

        let deficit = NeedReport::Fec {
            blocks: vec![NeedBlock {
                block_id: 0,
                deficit_symbols: 1,
            }],
        };
        sender.on_need(&mut shared, 22, 0, deficit.clone());
        sender.on_need(&mut shared, 23, 0, deficit);

        assert!(!sender.protocol_error);
        assert!(!sender.round_complete);
        assert_eq!(sender.phase, RoundPhase::WaitingForReports);
        assert!(sender.waiting_for_mettle_late_complete());
        assert!(
            sender.round_reports.len() == 2,
            "METTLE should not convert incomplete feedback into a session repair window"
        );

        sender.on_need(&mut shared, 22, 0, NeedReport::Complete);
        assert!(!sender.round_complete);
        sender.on_need(&mut shared, 23, 0, NeedReport::Complete);

        assert!(sender.round_complete);
        assert!(!sender.protocol_error);
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

    #[tokio::test]
    async fn mettle_sender_emits_finish_tail_after_real_object_prefix() {
        let manifest = LosslessSessionManifest {
            block_size: 16,
            total_bytes: 16,
            total_blocks: 1,
            mode: LosslessSessionMode::Fec(LosslessSessionFecMode::new_mettle(4, vec![7])),
        };
        let plan = BlockPlan::new(16, 16).expect("valid plan");
        let mut sender = FecSender::new(&manifest, plan).expect("METTLE sender");
        let source = Bytes::from(vec![0xA5; 16]);
        let shared = test_sender_shared_with_source(manifest, source);

        let payload = sender
            .source_symbol_payload(&shared, 0, 8)
            .expect("finish-tail METTLE bin");

        assert_eq!(payload.len(), 4);
        let stream = sender.blocks[0]
            .mettle_stream
            .as_ref()
            .expect("stream encoder should be initialized lazily");
        assert_eq!(stream.next_source_id, stream.real_source_count);
        assert!(
            stream.finished,
            "requesting a future bin should finish the terminated METTLE stream"
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
        }
    }
}
