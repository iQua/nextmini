use std::collections::BTreeMap;
use std::num::NonZeroUsize;
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
    /// RaptorQ needs retained payloads for block decode. METTLE streams bins
    /// directly into its decoder and leaves this map empty.
    pub(super) symbols: BTreeMap<u32, Vec<u8>>,
    pub(super) mettle: Option<MettleBlockDecodeState>,
}

pub(super) struct MettleBlockDecodeState {
    decoder: mettle::stream::Decoder,
    source_symbols: usize,
    initial_symbol_count: u32,
    terminal_symbol_count: u32,
    requested_repair_symbols: u32,
    decoded_source_count: usize,
}

enum MettleDecodeOutcome {
    Pending {
        decoded_sources: Vec<(usize, Vec<u8>)>,
    },
    Complete {
        decoded_sources: Vec<(usize, Vec<u8>)>,
    },
    InvalidSymbol,
}

impl MettleBlockDecodeState {
    fn new(source_symbols: usize, symbol_size: usize, seed: u64) -> Option<Self> {
        let source_symbol_bytes = NonZeroUsize::new(symbol_size)?;
        let metadata = mettle::block::BlockParams::new(source_symbols, symbol_size, seed)
            .metadata()
            .ok()?;
        Some(Self {
            decoder: mettle::stream::Decoder::new_terminated(
                mettle::MettleParams::new(mettle::OverheadRatio::DEFAULT),
                source_symbol_bytes,
                seed,
                source_symbols as u64,
            ),
            source_symbols,
            initial_symbol_count: u32::try_from(metadata.initial_symbol_count()).ok()?,
            terminal_symbol_count: u32::try_from(metadata.symbol_count()).ok()?,
            requested_repair_symbols: 0,
            decoded_source_count: 0,
        })
    }

    fn push_symbol(&mut self, symbol_id: u32, payload: Vec<u8>) -> MettleDecodeOutcome {
        let decoded = self.decoder.push_bin(u128::from(symbol_id), payload);
        let mut decoded_sources = Vec::with_capacity(decoded.len());
        for source in decoded {
            let (source_id, payload) = source.into_parts();
            let Ok(source_index) = usize::try_from(source_id) else {
                return MettleDecodeOutcome::InvalidSymbol;
            };
            if source_index != self.decoded_source_count || source_index >= self.source_symbols {
                return MettleDecodeOutcome::InvalidSymbol;
            }
            self.decoded_source_count += 1;
            decoded_sources.push((source_index, payload));
        }

        if self.decoded_source_count == self.source_symbols {
            MettleDecodeOutcome::Complete { decoded_sources }
        } else {
            MettleDecodeOutcome::Pending { decoded_sources }
        }
    }

    fn repair_deficit(&self) -> u16 {
        streaming_mettle_repair_deficit(
            self.source_symbols
                .saturating_sub(self.decoded_source_count),
            self.remaining_repair_symbols(),
        )
    }

    fn remaining_repair_symbols(&self) -> u32 {
        self.terminal_symbol_count
            .saturating_sub(self.initial_symbol_count)
            .saturating_sub(self.requested_repair_symbols)
    }

    fn record_repair_request(&mut self, requested: u16) {
        self.requested_repair_symbols = self
            .requested_repair_symbols
            .saturating_add(u32::from(requested));
    }
}

fn streaming_mettle_repair_deficit(remaining_sources: usize, remaining_repair_bins: u32) -> u16 {
    if remaining_sources == 0 || remaining_repair_bins == 0 {
        return 0;
    }
    let bounded = remaining_sources
        .min(remaining_repair_bins as usize)
        .min(usize::from(u16::MAX));
    u16::try_from(bounded).unwrap_or(u16::MAX).max(1)
}

fn mettle_terminal_repair_budget(source_symbols: usize, symbol_size: usize, seed: u64) -> u32 {
    let Ok(metadata) =
        mettle::block::BlockParams::new(source_symbols, symbol_size, seed).metadata()
    else {
        return 0;
    };
    let total = metadata.symbol_count();
    let initial = metadata.initial_symbol_count();
    u32::try_from(total.saturating_sub(initial)).unwrap_or(u32::MAX)
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
    decode_attempts: u64,
    decode_successes: u64,
    decode_insufficient_symbols: u64,
    decode_invalid_symbols: u64,
    decode_nanos: u128,
    decoded_source_symbols: u64,
    mettle_decoder_pushes: u64,
    mettle_decoder_completions: u64,
    mettle_decoder_invalid_symbols: u64,
    mettle_decoder_nanos: u128,
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

#[derive(Debug, Clone, Copy)]
enum DecodeStatus {
    Success,
    InsufficientSymbols,
    InvalidSymbol,
}

#[derive(Debug, Clone, Copy)]
enum MettleDecodeStatus {
    Pending,
    Complete,
    InvalidSymbol,
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
            decode_attempts: 0,
            decode_successes: 0,
            decode_insufficient_symbols: 0,
            decode_invalid_symbols: 0,
            decode_nanos: 0,
            decoded_source_symbols: 0,
            mettle_decoder_pushes: 0,
            mettle_decoder_completions: 0,
            mettle_decoder_invalid_symbols: 0,
            mettle_decoder_nanos: 0,
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

    fn record_accepted(&mut self, tree_id: u16, symbol_id: u32, symbols_per_block: u32) {
        self.accepted_symbols = self.accepted_symbols.saturating_add(1);
        let tree = self.per_tree.entry(tree_id).or_default();
        tree.accepted_symbols = tree.accepted_symbols.saturating_add(1);
        if symbol_id < symbols_per_block {
            self.source_symbols = self.source_symbols.saturating_add(1);
            tree.source_symbols = tree.source_symbols.saturating_add(1);
        } else {
            self.repair_symbols = self.repair_symbols.saturating_add(1);
            tree.repair_symbols = tree.repair_symbols.saturating_add(1);
        }
    }

    fn record_decode(&mut self, status: DecodeStatus, duration: Duration) {
        self.decode_attempts = self.decode_attempts.saturating_add(1);
        self.decode_nanos = self.decode_nanos.saturating_add(duration.as_nanos());
        match status {
            DecodeStatus::Success => {
                self.decode_successes = self.decode_successes.saturating_add(1);
            }
            DecodeStatus::InsufficientSymbols => {
                self.decode_insufficient_symbols =
                    self.decode_insufficient_symbols.saturating_add(1);
            }
            DecodeStatus::InvalidSymbol => {
                self.decode_invalid_symbols = self.decode_invalid_symbols.saturating_add(1);
            }
        }
    }

    fn record_decoded_sources(&mut self, decoded_sources: usize) {
        self.decoded_source_symbols = self
            .decoded_source_symbols
            .saturating_add(u64::try_from(decoded_sources).unwrap_or(u64::MAX));
    }

    fn record_mettle_decode(
        &mut self,
        status: MettleDecodeStatus,
        decoded_sources: usize,
        duration: Duration,
    ) {
        self.mettle_decoder_pushes = self.mettle_decoder_pushes.saturating_add(1);
        self.mettle_decoder_nanos = self
            .mettle_decoder_nanos
            .saturating_add(duration.as_nanos());
        self.record_decoded_sources(decoded_sources);
        match status {
            MettleDecodeStatus::Pending => {}
            MettleDecodeStatus::Complete => {
                self.mettle_decoder_completions = self.mettle_decoder_completions.saturating_add(1);
            }
            MettleDecodeStatus::InvalidSymbol => {
                self.mettle_decoder_invalid_symbols =
                    self.mettle_decoder_invalid_symbols.saturating_add(1);
            }
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

        let Some(scheme) = fec_mode.scheme_kind() else {
            self.stats.record_invalid(symbol.tree_id);
            return;
        };

        if scheme == FecScheme::Mettle {
            let payload = payload.to_vec();
            self.accept_mettle_symbol(shared, &fec_mode, &symbol);
            let _ = self
                .try_decode_mettle_symbol(shared, symbol.block_id, symbol.symbol_id, payload)
                .await;
        } else {
            if !self.accept_symbol(shared, &fec_mode, &symbol, payload.to_vec()) {
                return;
            }
            let _ = self
                .try_decode_fec_block(shared, symbol.block_id, &fec_mode)
                .await;
        }
    }

    fn accept_symbol(
        &mut self,
        shared: &mut super::ReceiverShared,
        fec_mode: &nextmini_messages::lossless_session::LosslessSessionFecMode,
        symbol: &nextmini_messages::lossless_session::LosslessSessionBlockSymbol,
        stored_payload: Vec<u8>,
    ) -> bool {
        let state = self.blocks.entry(symbol.block_id).or_default();
        if state.symbols.contains_key(&symbol.symbol_id) {
            self.stats.record_duplicate(symbol.tree_id);
            return false;
        }
        shared.mark_first_payload_unit();
        state.symbols.insert(symbol.symbol_id, stored_payload);
        self.stats
            .record_accepted(symbol.tree_id, symbol.symbol_id, fec_mode.symbols_per_block);
        self.maybe_log_progress(shared);
        true
    }

    fn accept_mettle_symbol(
        &mut self,
        shared: &mut super::ReceiverShared,
        fec_mode: &nextmini_messages::lossless_session::LosslessSessionFecMode,
        symbol: &nextmini_messages::lossless_session::LosslessSessionBlockSymbol,
    ) {
        self.blocks.entry(symbol.block_id).or_default();
        shared.mark_first_payload_unit();
        self.stats
            .record_accepted(symbol.tree_id, symbol.symbol_id, fec_mode.symbols_per_block);
        self.maybe_log_progress(shared);
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
        let Ok(source_symbols) = usize::try_from(fec_mode.symbols_per_block) else {
            return false;
        };
        if block_state.symbols.len() < source_symbols {
            return false;
        }
        let Some(block_len) = plan.block_len(block_id) else {
            return false;
        };
        let Some(scheme) = fec_mode.scheme_kind() else {
            return false;
        };

        if scheme != FecScheme::Mettle
            && let Some(block) = systematic_block_payload(
                block_state,
                source_symbols,
                self.geometry.symbol_size(),
                block_len,
            )
        {
            self.complete_block(shared, block_id, block).await;
            return true;
        }

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
            // For METTLE this split is only the legacy lossless-session API
            // boundary; both branches still map to coded bin ids.
            if symbol_id < fec_mode.symbols_per_block {
                received.push(decoder.source_symbol(symbol_id, payload.clone()));
            } else {
                received.push(decoder.coded_symbol(symbol_id, payload.clone()));
            }
        }

        let decode_started = Instant::now();
        let decode_result = decoder.decode(&received);
        let decode_status = match &decode_result {
            Ok(_) => DecodeStatus::Success,
            Err(session_fec::DecodeError::InsufficientSymbols) => DecodeStatus::InsufficientSymbols,
            Err(session_fec::DecodeError::InvalidSymbol) => DecodeStatus::InvalidSymbol,
        };
        self.stats
            .record_decode(decode_status, decode_started.elapsed());
        let Ok(output) = decode_result else {
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

    async fn try_decode_mettle_symbol(
        &mut self,
        shared: &mut super::ReceiverShared,
        block_id: u64,
        symbol_id: u32,
        payload: Vec<u8>,
    ) -> bool {
        let Some(plan) = shared.plan else {
            return false;
        };
        if plan.block_span(block_id).is_none() {
            return false;
        }
        let Some(manifest) = shared.manifest.as_ref() else {
            return false;
        };
        let LosslessSessionMode::Fec(fec_mode) = &manifest.mode else {
            return false;
        };
        let Ok(source_symbols) = usize::try_from(fec_mode.symbols_per_block) else {
            return false;
        };
        let Some(state) = self.blocks.get_mut(&block_id) else {
            return false;
        };
        if state.mettle.is_none() {
            let Some(mettle) = MettleBlockDecodeState::new(
                source_symbols,
                self.geometry.symbol_size(),
                session_fec::block_seed(shared.session_id, block_id),
            ) else {
                self.stats.record_mettle_decode(
                    MettleDecodeStatus::InvalidSymbol,
                    0,
                    Duration::ZERO,
                );
                return false;
            };
            state.mettle = Some(mettle);
        }

        let decode_started = Instant::now();
        let outcome = state
            .mettle
            .as_mut()
            .expect("METTLE decoder just initialized")
            .push_symbol(symbol_id, payload);
        let decode_elapsed = decode_started.elapsed();
        match outcome {
            MettleDecodeOutcome::Pending { decoded_sources } => {
                let decoded_count = decoded_sources.len();
                self.stats.record_mettle_decode(
                    MettleDecodeStatus::Pending,
                    decoded_count,
                    decode_elapsed,
                );
                self.write_decoded_mettle_sources(shared, block_id, decoded_sources)
                    .await;
                false
            }
            MettleDecodeOutcome::InvalidSymbol => {
                self.stats.record_mettle_decode(
                    MettleDecodeStatus::InvalidSymbol,
                    0,
                    decode_elapsed,
                );
                false
            }
            MettleDecodeOutcome::Complete { decoded_sources } => {
                let decoded_count = decoded_sources.len();
                self.stats.record_mettle_decode(
                    MettleDecodeStatus::Complete,
                    decoded_count,
                    decode_elapsed,
                );
                self.write_decoded_mettle_sources(shared, block_id, decoded_sources)
                    .await;
                self.complete_mettle_block(shared, block_id).await;
                true
            }
        }
    }

    async fn write_decoded_mettle_sources(
        &self,
        shared: &super::ReceiverShared,
        block_id: u64,
        decoded_sources: Vec<(usize, Vec<u8>)>,
    ) {
        for (source_index, payload) in decoded_sources {
            shared
                .write_symbol(block_id, self.geometry, source_index, &payload)
                .await;
        }
    }

    async fn complete_mettle_block(&mut self, shared: &mut super::ReceiverShared, block_id: u64) {
        shared.complete_blocks.insert(block_id);
        self.blocks.remove(&block_id);
        if shared.has_all_blocks() {
            shared.mark_object_complete();
            self.log_tree_stats(shared, "object_complete");
        }
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
        let Some(scheme) = fec_mode.scheme_kind() else {
            return 1;
        };
        let Ok(total) = usize::try_from(fec_mode.symbols_per_block) else {
            return 1;
        };
        if scheme == FecScheme::Mettle {
            return self
                .blocks
                .get(&block_id)
                .and_then(|state| state.mettle.as_ref())
                .map(MettleBlockDecodeState::repair_deficit)
                .unwrap_or_else(|| {
                    streaming_mettle_repair_deficit(
                        total,
                        mettle_terminal_repair_budget(
                            total,
                            self.geometry.symbol_size(),
                            session_fec::block_seed(shared.session_id, block_id),
                        ),
                    )
                });
        }
        let present = self
            .blocks
            .get(&block_id)
            .map(|state| state.symbols.len())
            .unwrap_or_default();
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
        self.record_reported_mettle_repairs(shared, &report);
        self.last_source_done_round_id = Some(round_id);
        self.last_round_need = Some(report.clone());
        shared.send_fec_need(round_id, &report).await;
        self.complete_reported = matches!(report, NeedReport::Complete);
        self.log_tree_stats(shared, "source_done");
    }

    fn record_reported_mettle_repairs(
        &mut self,
        shared: &super::ReceiverShared,
        report: &NeedReport,
    ) {
        let Some(manifest) = shared.manifest.as_ref() else {
            return;
        };
        let LosslessSessionMode::Fec(fec_mode) = &manifest.mode else {
            return;
        };
        if fec_mode.scheme_kind() != Some(FecScheme::Mettle) {
            return;
        }
        let NeedReport::Fec { blocks } = report else {
            return;
        };
        let Ok(source_symbols) = usize::try_from(fec_mode.symbols_per_block) else {
            return;
        };
        for block in blocks {
            let state = self.blocks.entry(block.block_id).or_default();
            if state.mettle.is_none() {
                let Some(mettle) = MettleBlockDecodeState::new(
                    source_symbols,
                    self.geometry.symbol_size(),
                    session_fec::block_seed(shared.session_id, block.block_id),
                ) else {
                    continue;
                };
                state.mettle = Some(mettle);
            }
            if let Some(mettle) = state.mettle.as_mut() {
                mettle.record_repair_request(block.deficit_symbols);
            }
        }
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
            decode_attempts = self.stats.decode_attempts,
            decode_successes = self.stats.decode_successes,
            decode_insufficient_symbols = self.stats.decode_insufficient_symbols,
            decode_invalid_symbols = self.stats.decode_invalid_symbols,
            decode_nanos = saturating_u128_to_u64(self.stats.decode_nanos),
            decode_avg_nanos = average_nanos(self.stats.decode_nanos, self.stats.decode_attempts),
            decoded_source_symbols = self.stats.decoded_source_symbols,
            mettle_decoder_pushes = self.stats.mettle_decoder_pushes,
            mettle_decoder_completions = self.stats.mettle_decoder_completions,
            mettle_decoder_invalid_symbols = self.stats.mettle_decoder_invalid_symbols,
            mettle_decoder_nanos = saturating_u128_to_u64(self.stats.mettle_decoder_nanos),
            mettle_decoder_avg_nanos = average_nanos(
                self.stats.mettle_decoder_nanos,
                self.stats.mettle_decoder_pushes
            ),
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
            decode_attempts = self.stats.decode_attempts,
            decode_successes = self.stats.decode_successes,
            decode_insufficient_symbols = self.stats.decode_insufficient_symbols,
            decode_invalid_symbols = self.stats.decode_invalid_symbols,
            decode_nanos = saturating_u128_to_u64(self.stats.decode_nanos),
            decode_avg_nanos = average_nanos(self.stats.decode_nanos, self.stats.decode_attempts),
            decoded_source_symbols = self.stats.decoded_source_symbols,
            mettle_decoder_pushes = self.stats.mettle_decoder_pushes,
            mettle_decoder_completions = self.stats.mettle_decoder_completions,
            mettle_decoder_invalid_symbols = self.stats.mettle_decoder_invalid_symbols,
            mettle_decoder_nanos = saturating_u128_to_u64(self.stats.mettle_decoder_nanos),
            mettle_decoder_avg_nanos = average_nanos(
                self.stats.mettle_decoder_nanos,
                self.stats.mettle_decoder_pushes
            ),
            tree_stats = %self.stats.tree_summary(),
            "Lossless FEC receiver per-tree stats"
        );
    }
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
