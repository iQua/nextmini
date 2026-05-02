use bytes::Bytes;
use std::collections::BTreeMap;
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
use crate::node::session::plan::{BlockPlan, SymbolGeometry};

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
    symbols_per_block: u16,
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
        let block_count =
            usize::try_from(plan.total_blocks()).map_err(|_| "too many blocks for fec sender")?;

        Ok(Self {
            blocks: (0..block_count)
                .map(|_| FecBlockState {
                    next_source_symbol: 0,
                    next_fountain_symbol: u32::from(fec.symbols_per_block),
                    required_extra_symbols: 0,
                    emitted_extra_symbols: 0,
                    encoder: None,
                })
                .collect(),
            scheme,
            symbols_per_block: fec.symbols_per_block,
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
        let symbols_per_block = u32::from(self.symbols_per_block);
        for (block_idx, block) in self.blocks.iter_mut().enumerate() {
            let block_id = block_idx as u64;
            if block.next_source_symbol < symbols_per_block && self.phase == RoundPhase::SendingData
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
        let Some(payload) = self.source_symbol_payload(shared, block_id, symbol_id) else {
            return false;
        };
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
        if !self.try_send_symbol(shared, block_id, symbol_id, &payload, SymbolKind::Repair) {
            return false;
        }

        if let Some(block) = fec_block_mut(self, block_id) {
            block.emitted_extra_symbols = block.emitted_extra_symbols.saturating_add(1);
            block.next_fountain_symbol += 1;
        }
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

    /// Return the cached source symbol payload for one block and symbol index.
    fn source_symbol_payload(
        &mut self,
        shared: &super::SenderShared,
        block_id: u64,
        symbol_id: u32,
    ) -> Option<Bytes> {
        ensure_source_symbol_cache(&shared.source, shared.plan, self, block_id)?;
        let (_, symbols) = self.current_source_cache.as_ref()?;
        let idx = usize::try_from(symbol_id).ok()?;
        symbols.get(idx).cloned()
    }

    /// Lazily build an encoder and derive one extra fountain symbol payload.
    fn extra_symbol_payload(
        &mut self,
        shared: &super::SenderShared,
        block_id: u64,
        symbol_id: u32,
    ) -> Option<Vec<u8>> {
        let need_encoder = fec_block_ref(self, block_id).map(|block| block.encoder.is_none())?;

        if need_encoder {
            let span = shared.plan.block_span(block_id)?;
            let source_block = shared.source.padded_symbol_bytes(span, self.geometry);
            let params = BlockParams::with_scheme(
                usize::from(self.symbols_per_block),
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
            repair_window_symbols = self.repair_window_symbols,
            total_required_extra_symbols = self.total_required_extra_symbols(),
            total_emitted_extra_symbols = self.total_emitted_extra_symbols(),
            tree_stats = %self.stats.tree_summary(),
            "Lossless FEC sender per-tree stats"
        );
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
            Some((0, u32::from(sender.symbols_per_block))),
            "first quorum report should open a bounded speculative repair window"
        );
        if let Some(block) = sender.blocks.first_mut() {
            block.emitted_extra_symbols = 2;
            block.next_fountain_symbol = u32::from(sender.symbols_per_block) + 2;
        }
        assert_eq!(
            sender.next_extra_symbol(&shared),
            None,
            "speculative repair should stop once the partial window is exhausted"
        );

        sender.on_need(&mut shared, 23, 0, NeedReport::Complete);

        assert_eq!(
            sender.next_extra_symbol(&shared),
            Some((0, u32::from(sender.symbols_per_block) + 2)),
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

    fn test_manifest() -> LosslessSessionManifest {
        LosslessSessionManifest {
            block_size: 16,
            total_bytes: 16,
            total_blocks: 1,
            mode: LosslessSessionMode::Fec(LosslessSessionFecMode::new_raptorq(4, vec![7, 9])),
        }
    }

    fn test_sender_shared(manifest: LosslessSessionManifest) -> SenderShared {
        let processors = ProcessorHandle::new(LocalConfig {
            node_id: 0,
            n_nodes: 1,
            num_packet_processors: 1,
            channel_capacity: 8,
            user_space_base_addr: Ipv4Addr::new(10, 0, 0, 0),
            local_netmask: Ipv4Addr::new(255, 255, 255, 0),
            ..Default::default()
        });

        SenderShared {
            session: SessionConfig {
                session_id: 7,
                block_size: 16,
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
            plan: BlockPlan::new(16, 16).expect("valid plan"),
            source: BlockSource::new(Bytes::from_static(b"abcdefghijklmnop")),
            ready_grace: Duration::from_millis(1),
            topology_ready: None,
            pacer: None,
            payload_emitted: false,
        }
    }
}
