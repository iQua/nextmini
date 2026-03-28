use bytes::Bytes;
use std::collections::BTreeMap;
use tokio::sync::mpsc;
use tracing::warn;

use nextmini_messages::lossless_session::{
    self, LosslessSessionManifest, LosslessSessionMode, NeedReport,
};

use crate::node::processor::SendOutcome;
use crate::node::session::api::InboundFrame;
use crate::node::session::api::SessionOutcome;
use crate::node::session::control;
use crate::node::session::fec as session_fec;
use crate::node::session::fec::{BlockParams, Encoder};
use crate::node::session::plan::{BlockPlan, SymbolGeometry};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RoundPhase {
    SendingData,
    WaitingForReports,
}

/// FEC-mode sender state and scheduling cursors.
pub(super) struct FecSender {
    blocks: Vec<FecBlockState>,
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
    protocol_error: bool,
}

/// Per-block sender cursor and encoder state for FEC mode.
struct FecBlockState {
    next_source_symbol: u32,
    next_fountain_symbol: u32,
    extra_budget: u16,
    encoder: Option<Encoder>,
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
                    extra_budget: 0,
                    encoder: None,
                })
                .collect(),
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
            protocol_error: false,
        })
    }

    /// Main send loop for FEC mode.
    ///
    /// Source symbols are always sent before extra fountain symbols. The sender
    /// only emits extra symbols after it has completed a source-symbol sweep
    /// and received round-status feedback for every receiver.
    pub(super) async fn run(
        &mut self,
        shared: &mut super::SenderShared,
        ctrl_rx: &mut mpsc::Receiver<InboundFrame>,
    ) -> SessionOutcome {
        while !self.is_complete() {
            shared.drain_controls(ctrl_rx, self);
            if self.protocol_error {
                return SessionOutcome::Aborted;
            }

            if self.phase == RoundPhase::WaitingForReports {
                if self.round_reports.len() == shared.active_quorum.active_members().len() {
                    self.finish_report_round(shared);
                    continue;
                }
                match shared
                    .wait_for_quorum_feedback(ctrl_rx, self, self.current_round_id)
                    .await
                {
                    super::QuorumWaitOutcome::Control | super::QuorumWaitOutcome::Solicited => {}
                    super::QuorumWaitOutcome::TimedOut | super::QuorumWaitOutcome::Closed => {
                        return SessionOutcome::Aborted;
                    }
                }
                if self.protocol_error {
                    return SessionOutcome::Aborted;
                }
                continue;
            }

            if let Some((block_id, symbol_id)) = self.next_source_symbol(shared) {
                if self.send_source_symbol(shared, block_id, symbol_id).await {
                    self.round_source_done_sent = false;
                    continue;
                }
                if !shared.wait_for_signal(ctrl_rx, self).await {
                    return SessionOutcome::Aborted;
                }
                continue;
            }

            if let Some((block_id, symbol_id)) = self.next_extra_symbol(shared) {
                if self.send_extra_symbol(shared, block_id, symbol_id).await {
                    self.round_source_done_sent = false;
                    continue;
                }
                if !shared.wait_for_signal(ctrl_rx, self).await {
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

            match shared
                .wait_for_quorum_feedback(ctrl_rx, self, self.current_round_id)
                .await
            {
                super::QuorumWaitOutcome::Control | super::QuorumWaitOutcome::Solicited => {}
                super::QuorumWaitOutcome::TimedOut | super::QuorumWaitOutcome::Closed => {
                    return SessionOutcome::Aborted;
                }
            }
        }

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
        for (block_idx, block) in self.blocks.iter_mut().enumerate() {
            let block_id = block_idx as u64;
            if block.extra_budget > 0 && self.phase == RoundPhase::SendingData {
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
        if !self.try_send_symbol(shared, block_id, symbol_id, payload.as_ref()) {
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
            return false;
        };
        shared.pace(payload.len()).await;
        if !self.try_send_symbol(shared, block_id, symbol_id, &payload) {
            return false;
        }

        if let Some(block) = fec_block_mut(self, block_id) {
            block.extra_budget = block.extra_budget.saturating_sub(1);
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
    ) -> bool {
        if self.tree_ids.is_empty() {
            return false;
        }

        let tree_count = self.tree_ids.len();
        let start_idx = self.next_tree_rr;
        let initial_tree_id = self.tree_ids[start_idx];
        lossless_session::encode_block_symbol_into(
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
                lossless_session::set_block_symbol_tree_id(&mut self.frame_scratch, tree_id)
                    .expect("encoded block symbol should accept tree-id patch");
            }
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
            match submission.outcome {
                SendOutcome::Queued => {
                    self.next_tree_rr = (idx + 1) % tree_count;
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
            let params = BlockParams::new(
                usize::from(self.symbols_per_block),
                self.geometry.symbol_size(),
                session_fec::block_seed(shared.session.session_id, block_id),
            );
            let block = fec_block_mut(self, block_id)?;
            if block.encoder.is_none() {
                block.encoder = Encoder::from_block(params, source_block.as_ref());
            }
        }

        fec_block_ref(self, block_id)
            .and_then(|block| block.encoder.as_ref())
            .map(|encoder| encoder.coded_symbol(symbol_id))
    }

    async fn begin_report_round(&mut self, shared: &mut super::SenderShared) {
        if self.phase == RoundPhase::WaitingForReports {
            return;
        }
        shared.send_source_done(self.current_round_id).await;
        self.round_reports.clear();
        self.phase = RoundPhase::WaitingForReports;
    }

    fn finish_report_round(&mut self, shared: &mut super::SenderShared) {
        let mut aggregated = vec![0u16; self.blocks.len()];
        let mut all_complete = true;

        for status in self.round_reports.values() {
            match status {
                NeedReport::Complete => {}
                NeedReport::Fec { blocks } => {
                    all_complete = false;
                    for block in blocks {
                        let Some(entry) = aggregated
                            .get_mut(usize::try_from(block.block_id).ok().unwrap_or(usize::MAX))
                        else {
                            continue;
                        };
                        *entry = (*entry).max(block.deficit_symbols);
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
            return;
        }

        for (block, budget) in self.blocks.iter_mut().zip(aggregated) {
            block.extra_budget = budget;
        }
        self.round_reports.clear();
        shared.clear_quorum_feedback_wait();
        self.phase = RoundPhase::SendingData;
        self.current_round_id = self.current_round_id.saturating_add(1);
        self.round_source_done_sent = false;
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
        if self.phase != RoundPhase::WaitingForReports || round_id != self.current_round_id {
            return;
        }
        if let Some(existing) = self.round_reports.get(&peer_id) {
            if existing != &report {
                self.protocol_error = true;
            }
            return;
        }
        self.round_reports.insert(peer_id, report);
        if self.round_reports.len() == shared.active_quorum.active_members().len() {
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
