use std::collections::BTreeMap;
use std::time::{Duration, Instant};

use nextmini_messages::lossless_session::{
    self, FecScheme, LosslessSessionMode, NeedBlock, NeedReport,
};
use tracing::{info, warn};

use crate::node::session::api::InboundFrame;
use crate::node::session::fec as session_fec;
use crate::node::session::fec::{BlockParams, Decoder};
use crate::node::session::plan::SymbolGeometry;

const FEC_RECEIVER_PROGRESS_LOG_INTERVAL: Duration = Duration::from_secs(1);
const FEC_RECEIVER_PROGRESS_SYMBOL_INTERVAL: u64 = 4096;

/// Accumulated FEC symbols for one logical block.
#[derive(Default)]
pub(super) struct FecBlockState {
    pub(super) symbols: BTreeMap<u32, Vec<u8>>,
}

/// FEC-mode receiver state machine.
pub(super) struct FecReceiver {
    pub(super) geometry: SymbolGeometry,
    pub(super) blocks: BTreeMap<u64, FecBlockState>,
    pub(super) last_source_done_round_id: Option<u32>,
    pub(super) last_round_need: Option<NeedReport>,
    complete_reported: bool,
    stats: FecReceiverStats,
}

#[derive(Debug)]
struct FecReceiverStats {
    per_tree: BTreeMap<u16, FecTreeReceiveStats>,
    accepted_symbols: u64,
    source_symbols: u64,
    repair_symbols: u64,
    duplicate_symbols: u64,
    complete_block_symbols: u64,
    invalid_symbols: u64,
    last_progress_at: Instant,
    last_progress_accepted: u64,
}

#[derive(Debug, Default)]
struct FecTreeReceiveStats {
    accepted_symbols: u64,
    source_symbols: u64,
    repair_symbols: u64,
    duplicate_symbols: u64,
    complete_block_symbols: u64,
    invalid_symbols: u64,
}

impl FecReceiverStats {
    fn new() -> Self {
        Self {
            per_tree: BTreeMap::new(),
            accepted_symbols: 0,
            source_symbols: 0,
            repair_symbols: 0,
            duplicate_symbols: 0,
            complete_block_symbols: 0,
            invalid_symbols: 0,
            last_progress_at: Instant::now(),
            last_progress_accepted: 0,
        }
    }

    fn record_invalid(&mut self, tree_id: u16) {
        self.invalid_symbols = self.invalid_symbols.saturating_add(1);
        let tree = self.per_tree.entry(tree_id).or_default();
        tree.invalid_symbols = tree.invalid_symbols.saturating_add(1);
    }

    fn record_complete_block(&mut self, tree_id: u16) {
        self.complete_block_symbols = self.complete_block_symbols.saturating_add(1);
        let tree = self.per_tree.entry(tree_id).or_default();
        tree.complete_block_symbols = tree.complete_block_symbols.saturating_add(1);
    }

    fn record_duplicate(&mut self, tree_id: u16) {
        self.duplicate_symbols = self.duplicate_symbols.saturating_add(1);
        let tree = self.per_tree.entry(tree_id).or_default();
        tree.duplicate_symbols = tree.duplicate_symbols.saturating_add(1);
    }

    fn record_accepted(&mut self, tree_id: u16, symbol_id: u32, symbols_per_block: u16) {
        self.accepted_symbols = self.accepted_symbols.saturating_add(1);
        let tree = self.per_tree.entry(tree_id).or_default();
        tree.accepted_symbols = tree.accepted_symbols.saturating_add(1);
        if symbol_id < u32::from(symbols_per_block) {
            self.source_symbols = self.source_symbols.saturating_add(1);
            tree.source_symbols = tree.source_symbols.saturating_add(1);
        } else {
            self.repair_symbols = self.repair_symbols.saturating_add(1);
            tree.repair_symbols = tree.repair_symbols.saturating_add(1);
        }
    }

    fn tree_summary(&self) -> String {
        self.per_tree
            .iter()
            .map(|(tree_id, stats)| {
                format!(
                    "{}:acc={},src={},rep={},dup={},done={},invalid={}",
                    tree_id,
                    stats.accepted_symbols,
                    stats.source_symbols,
                    stats.repair_symbols,
                    stats.duplicate_symbols,
                    stats.complete_block_symbols,
                    stats.invalid_symbols
                )
            })
            .collect::<Vec<_>>()
            .join(";")
    }
}

impl FecReceiver {
    pub(super) fn last_source_done_round_id(&self) -> Option<u32> {
        self.last_source_done_round_id
    }

    /// Build receiver-side FEC state from the negotiated symbol geometry.
    pub(super) fn new(geometry: SymbolGeometry) -> Self {
        Self {
            geometry,
            blocks: BTreeMap::new(),
            last_source_done_round_id: None,
            last_round_need: None,
            complete_reported: false,
            stats: FecReceiverStats::new(),
        }
    }

    /// Handle one FEC symbol frame.
    pub(super) async fn handle_block_symbol_frame(
        &mut self,
        shared: &mut super::ReceiverShared,
        frame: InboundFrame,
    ) {
        let Some(manifest) = shared.manifest.as_ref() else {
            return;
        };
        let LosslessSessionMode::Fec(fec_mode) = manifest.mode.clone() else {
            return;
        };
        let Some((_, symbol, payload)) = lossless_session::decode_block_symbol(&frame.bytes) else {
            return;
        };
        if manifest.validate_block_symbol(&symbol).is_err() {
            self.stats.record_invalid(symbol.tree_id);
            return;
        }
        if payload.len() != self.geometry.symbol_size() {
            self.stats.record_invalid(symbol.tree_id);
            return;
        }
        if shared.complete_blocks.contains(&symbol.block_id) {
            self.stats.record_complete_block(symbol.tree_id);
            return;
        }

        let state = self.blocks.entry(symbol.block_id).or_default();
        if state.symbols.contains_key(&symbol.symbol_id) {
            self.stats.record_duplicate(symbol.tree_id);
            return;
        }
        shared.mark_first_payload_unit();
        state.symbols.insert(symbol.symbol_id, payload.to_vec());
        self.stats
            .record_accepted(symbol.tree_id, symbol.symbol_id, fec_mode.symbols_per_block);
        self.maybe_log_progress(shared);

        let _ = self
            .try_decode_fec_block(shared, symbol.block_id, &fec_mode)
            .await;
    }

    /// Attempt to decode a complete-enough FEC block.
    async fn try_decode_fec_block(
        &mut self,
        shared: &mut super::ReceiverShared,
        block_id: u64,
        fec_mode: &nextmini_messages::lossless_session::LosslessSessionFecMode,
    ) -> bool {
        let Some(plan) = shared.plan else {
            return false;
        };
        let Some(block_state) = self.blocks.get(&block_id) else {
            return false;
        };
        let source_symbols = usize::from(fec_mode.symbols_per_block);
        if block_state.symbols.len() < source_symbols {
            return false;
        }
        let Some(block_len) = plan.block_len(block_id) else {
            return false;
        };

        if let Some(block) = systematic_block_payload(
            block_state,
            source_symbols,
            self.geometry.symbol_size(),
            block_len,
        ) {
            self.complete_block(shared, block_id, block).await;
            return true;
        }

        let Some(scheme) = fec_mode.scheme_kind() else {
            return false;
        };
        let params = BlockParams::with_scheme(
            source_symbols,
            self.geometry.symbol_size(),
            session_fec::block_seed(shared.session_id, block_id),
            scheme,
        );
        let decoder = Decoder::from_block(params);
        let mut received = Vec::with_capacity(block_state.symbols.len());

        for (&symbol_id, payload) in &block_state.symbols {
            if payload.len() != self.geometry.symbol_size() {
                return false;
            }
            if symbol_id < u32::from(fec_mode.symbols_per_block) {
                received.push(decoder.source_symbol(symbol_id, payload.clone()));
            } else {
                received.push(decoder.coded_symbol(symbol_id, payload.clone()));
            }
        }

        let Ok(output) = decoder.decode(&received) else {
            return false;
        };

        let Some(block_capacity) = output
            .source_symbols
            .len()
            .checked_mul(self.geometry.symbol_size())
        else {
            warn!(
                session_id = shared.session_id,
                block_id,
                symbol_size = self.geometry.symbol_size(),
                "Lossless receiver overflowed FEC block allocation geometry"
            );
            return false;
        };
        let mut block = Vec::new();
        if block.try_reserve_exact(block_capacity).is_err() {
            warn!(
                session_id = shared.session_id,
                block_id,
                block_capacity,
                "Lossless receiver failed to reserve space for decoded FEC block"
            );
            return false;
        }
        for symbol in output.source_symbols {
            block.extend_from_slice(&symbol);
        }
        block.truncate(block_len);

        self.complete_block(shared, block_id, block).await;
        true
    }

    async fn complete_block(
        &mut self,
        shared: &mut super::ReceiverShared,
        block_id: u64,
        block: Vec<u8>,
    ) {
        shared.write_block(block_id, &block).await;
        shared.complete_blocks.insert(block_id);
        self.blocks.remove(&block_id);
        if shared.has_all_blocks() {
            shared.mark_object_complete();
            self.log_tree_stats(shared, "object_complete");
        }
    }

    /// Compute how many additional source-equivalent symbols are still needed.
    pub(super) fn block_deficit(&self, shared: &super::ReceiverShared, block_id: u64) -> u16 {
        let Some(manifest) = shared.manifest.as_ref() else {
            return 1;
        };
        let LosslessSessionMode::Fec(fec_mode) = &manifest.mode else {
            return 1;
        };
        let present = self
            .blocks
            .get(&block_id)
            .map(|state| state.symbols.keys().copied().collect::<Vec<_>>())
            .unwrap_or_default();
        let Some(scheme) = fec_mode.scheme_kind() else {
            return 1;
        };
        let total = usize::from(fec_mode.symbols_per_block);
        if scheme == FecScheme::Mettle {
            let params = BlockParams::with_scheme(
                total,
                self.geometry.symbol_size(),
                session_fec::block_seed(shared.session_id, block_id),
                scheme,
            );
            return session_fec::repair_deficit(params, present);
        }
        let present = present.len();
        if present >= total {
            1
        } else {
            u16::try_from(total - present).unwrap_or(u16::MAX).max(1)
        }
    }

    pub(super) fn need_report(&self, shared: &super::ReceiverShared) -> Option<NeedReport> {
        let plan = shared.plan?;
        if plan.total_blocks() == 0 || shared.has_all_blocks() {
            return Some(NeedReport::Complete);
        }
        let mut blocks = Vec::new();
        for block_id in 0..plan.total_blocks() {
            if shared.complete_blocks.contains(&block_id) {
                continue;
            }
            blocks.push(NeedBlock {
                block_id,
                deficit_symbols: self.block_deficit(shared, block_id),
            });
        }
        Some(NeedReport::Fec { blocks })
    }

    pub(super) async fn handle_source_done(
        &mut self,
        shared: &super::ReceiverShared,
        round_id: u32,
    ) {
        if let Some(last_round_id) = self.last_source_done_round_id {
            if round_id < last_round_id {
                return;
            }
            if round_id == last_round_id {
                if let Some(report) = self.last_round_need.clone() {
                    shared.send_fec_need(last_round_id, &report).await;
                    self.complete_reported = matches!(report, NeedReport::Complete);
                }
                return;
            }
        }

        let Some(report) = self.need_report(shared) else {
            return;
        };
        self.last_source_done_round_id = Some(round_id);
        self.last_round_need = Some(report.clone());
        shared.send_fec_need(round_id, &report).await;
        self.complete_reported = matches!(report, NeedReport::Complete);
        self.log_tree_stats(shared, "source_done");
    }

    pub(super) fn is_complete(&self) -> bool {
        self.complete_reported
    }

    fn maybe_log_progress(&mut self, shared: &super::ReceiverShared) {
        let now = Instant::now();
        let accepted_delta = self
            .stats
            .accepted_symbols
            .saturating_sub(self.stats.last_progress_accepted);
        if accepted_delta == 0 {
            return;
        }
        let interval_due =
            now.duration_since(self.stats.last_progress_at) >= FEC_RECEIVER_PROGRESS_LOG_INTERVAL;
        let symbol_due = accepted_delta >= FEC_RECEIVER_PROGRESS_SYMBOL_INTERVAL;
        if !interval_due && !symbol_due {
            return;
        }

        info!(
            session_id = shared.session_id,
            local_node_id = shared.local_node_id,
            accepted_symbols = self.stats.accepted_symbols,
            source_symbols = self.stats.source_symbols,
            repair_symbols = self.stats.repair_symbols,
            duplicate_symbols = self.stats.duplicate_symbols,
            complete_block_symbols = self.stats.complete_block_symbols,
            invalid_symbols = self.stats.invalid_symbols,
            accepted_delta,
            tree_stats = %self.stats.tree_summary(),
            "Lossless FEC receiver per-tree progress"
        );

        self.stats.last_progress_at = now;
        self.stats.last_progress_accepted = self.stats.accepted_symbols;
    }

    fn log_tree_stats(&self, shared: &super::ReceiverShared, reason: &'static str) {
        info!(
            session_id = shared.session_id,
            local_node_id = shared.local_node_id,
            reason,
            accepted_symbols = self.stats.accepted_symbols,
            source_symbols = self.stats.source_symbols,
            repair_symbols = self.stats.repair_symbols,
            duplicate_symbols = self.stats.duplicate_symbols,
            complete_block_symbols = self.stats.complete_block_symbols,
            invalid_symbols = self.stats.invalid_symbols,
            tree_stats = %self.stats.tree_summary(),
            "Lossless FEC receiver per-tree stats"
        );
    }
}

fn systematic_block_payload(
    block_state: &FecBlockState,
    source_symbols: usize,
    symbol_size: usize,
    block_len: usize,
) -> Option<Vec<u8>> {
    let block_capacity = source_symbols.checked_mul(symbol_size)?;
    let mut block = Vec::new();
    block.try_reserve_exact(block_capacity).ok()?;

    for symbol_id in 0..source_symbols as u32 {
        let payload = block_state.symbols.get(&symbol_id)?;
        if payload.len() != symbol_size {
            return None;
        }
        block.extend_from_slice(payload);
    }

    block.truncate(block_len);
    Some(block)
}
