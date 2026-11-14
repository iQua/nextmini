use std::collections::BTreeMap;
use std::num::NonZeroUsize;
use std::sync::Arc;

use nextmini_messages::lossless_session::{
    self, FecScheme, LosslessSessionMode, NeedBlock, NeedReport, max_fec_need_blocks,
};
use tracing::warn;

use crate::node::packet::{LOSSLESS_TRANSPORT_OVERHEAD, MAX_FRAMED_PACKET_SIZE};
use crate::node::session::api::InboundFrame;
use crate::node::session::fec as session_fec;
use crate::node::session::fec::{BlockParams, Decoder};
use crate::node::session::plan::SymbolGeometry;

const METTLE_DECODED_WRITE_BATCH_BYTES: usize = 1024 * 1024;

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
    decoded_source_count: usize,
}

enum MettleDecodeOutcome {
    Pending {
        decoded_sources: Vec<(usize, Arc<Vec<u8>>)>,
    },
    Complete {
        decoded_sources: Vec<(usize, Arc<Vec<u8>>)>,
    },
    InvalidSymbol,
}

impl MettleBlockDecodeState {
    fn new(
        source_symbols: usize,
        symbol_size: usize,
        seed: u64,
        mettle_overhead: mettle::OverheadRatio,
    ) -> Option<Self> {
        let source_symbol_bytes = NonZeroUsize::new(symbol_size)?;
        let terminal_source_count = u64::try_from(source_symbols).ok()?;
        Some(Self {
            decoder: mettle::stream::Decoder::new_terminated(
                mettle::MettleParams::new(mettle_overhead),
                source_symbol_bytes,
                seed,
                terminal_source_count,
            ),
            source_symbols,
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
            if source_index >= self.source_symbols {
                if self.decoded_source_count == self.source_symbols {
                    continue;
                }
                return MettleDecodeOutcome::InvalidSymbol;
            }
            if source_index != self.decoded_source_count {
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
        )
    }
}

fn streaming_mettle_repair_deficit(remaining_sources: usize) -> u16 {
    if remaining_sources == 0 {
        return 0;
    }
    // METTLE emits the paper finite stream once. A non-complete report is a
    // completion probe response, not a request for session-estimated repairs.
    1
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
    decoded_source_symbols: u64,
    mettle_decoder_pushes: u64,
    mettle_decoder_completions: u64,
    mettle_decoder_invalid_symbols: u64,
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
            decoded_source_symbols: 0,
            mettle_decoder_pushes: 0,
            mettle_decoder_completions: 0,
            mettle_decoder_invalid_symbols: 0,
        }
    }

    fn record_invalid(&mut self, _tree_id: u16) {
        self.invalid_symbols = self.invalid_symbols.saturating_add(1);
    }

    fn record_complete_block(&mut self, _tree_id: u16) {
        self.complete_block_symbols = self.complete_block_symbols.saturating_add(1);
    }

    fn record_duplicate(&mut self, _tree_id: u16) {
        self.duplicate_symbols = self.duplicate_symbols.saturating_add(1);
    }

    fn record_accepted(&mut self, _tree_id: u16, symbol_id: u32, symbols_per_block: u32) {
        self.accepted_symbols = self.accepted_symbols.saturating_add(1);
        if symbol_id < symbols_per_block {
            self.source_symbols = self.source_symbols.saturating_add(1);
        } else {
            self.repair_symbols = self.repair_symbols.saturating_add(1);
        }
    }

    fn record_decode(&mut self, status: DecodeStatus) {
        self.decode_attempts = self.decode_attempts.saturating_add(1);
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

    fn record_mettle_decode(&mut self, status: MettleDecodeStatus, decoded_sources: usize) {
        self.mettle_decoder_pushes = self.mettle_decoder_pushes.saturating_add(1);
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
}

impl FecReceiver {
    pub(super) fn last_source_done_round_id(&self) -> Option<u32> {
        self.last_source_done_round_id
    }

    /// Build receiver-side FEC state from the negotiated symbol geometry.
    pub(super) fn new(geometry: SymbolGeometry) -> Self {
        assert_eq!(
            max_fec_need_blocks(MAX_FRAMED_PACKET_SIZE, LOSSLESS_TRANSPORT_OVERHEAD),
            Some(6545),
            "FEC Need sizing must match the lossless packet envelope"
        );
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
                .try_decode_mettle_symbol(
                    shared,
                    &fec_mode,
                    symbol.block_id,
                    symbol.symbol_id,
                    symbol.tree_id,
                    payload,
                )
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
            // RaptorQ's initial ESI range is systematic source data; later
            // ESIs are repair symbols from the same source block encoder.
            if symbol_id < fec_mode.symbols_per_block {
                received.push(decoder.source_symbol(symbol_id, payload.clone()));
            } else {
                received.push(decoder.coded_symbol(symbol_id, payload.clone()));
            }
        }

        let decode_result = decoder.decode(&received);
        let decode_status = match &decode_result {
            Ok(_) => DecodeStatus::Success,
            Err(session_fec::DecodeError::InsufficientSymbols) => DecodeStatus::InsufficientSymbols,
            Err(session_fec::DecodeError::InvalidSymbol) => DecodeStatus::InvalidSymbol,
        };
        self.stats.record_decode(decode_status);
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
        fec_mode: &nextmini_messages::lossless_session::LosslessSessionFecMode,
        block_id: u64,
        symbol_id: u32,
        _tree_id: u16,
        payload: Vec<u8>,
    ) -> bool {
        let Some(plan) = shared.plan else {
            return false;
        };
        if plan.block_span(block_id).is_none() {
            return false;
        }
        let Ok(source_symbols) = usize::try_from(fec_mode.symbols_per_block) else {
            return false;
        };
        let needs_mettle = self
            .blocks
            .get(&block_id)
            .map(|state| state.mettle.is_none())
            .unwrap_or(false);
        if needs_mettle {
            let Some(mettle_overhead) = session_fec::mettle_overhead_from_fec_mode(fec_mode) else {
                self.stats
                    .record_mettle_decode(MettleDecodeStatus::InvalidSymbol, 0);
                return false;
            };
            let Some(mettle) = MettleBlockDecodeState::new(
                source_symbols,
                self.geometry.symbol_size(),
                session_fec::block_seed(shared.session_id, block_id),
                mettle_overhead,
            ) else {
                self.stats
                    .record_mettle_decode(MettleDecodeStatus::InvalidSymbol, 0);
                return false;
            };
            let Some(state) = self.blocks.get_mut(&block_id) else {
                return false;
            };
            state.mettle = Some(mettle);
        }

        let Some(state) = self.blocks.get_mut(&block_id) else {
            return false;
        };
        let mettle = state
            .mettle
            .as_mut()
            .expect("METTLE decoder just initialized");
        let outcome = mettle.push_symbol(symbol_id, payload);
        match outcome {
            MettleDecodeOutcome::Pending { decoded_sources } => {
                let decoded_count = decoded_sources.len();
                self.stats
                    .record_mettle_decode(MettleDecodeStatus::Pending, decoded_count);
                self.write_decoded_mettle_sources(shared, block_id, decoded_sources)
                    .await;
                false
            }
            MettleDecodeOutcome::InvalidSymbol => {
                self.stats
                    .record_mettle_decode(MettleDecodeStatus::InvalidSymbol, 0);
                false
            }
            MettleDecodeOutcome::Complete { decoded_sources } => {
                let decoded_count = decoded_sources.len();
                self.stats
                    .record_mettle_decode(MettleDecodeStatus::Complete, decoded_count);
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
        decoded_sources: Vec<(usize, Arc<Vec<u8>>)>,
    ) {
        let mut run_start = None;
        let mut expected_source_index = None;
        let mut run_payload = Vec::new();

        for (source_index, payload) in decoded_sources {
            let continues_run = expected_source_index == Some(source_index);
            let fits_batch =
                run_payload.len().saturating_add(payload.len()) <= METTLE_DECODED_WRITE_BATCH_BYTES;
            if !continues_run || !fits_batch {
                if let Some(start) = run_start.take() {
                    shared
                        .write_symbol_run(block_id, self.geometry, start, &run_payload)
                        .await;
                    run_payload.clear();
                }
            }

            if run_start.is_none() {
                run_start = Some(source_index);
            }
            run_payload.extend_from_slice(payload.as_slice());
            expected_source_index = Some(source_index.saturating_add(1));
        }

        if let Some(start) = run_start {
            shared
                .write_symbol_run(block_id, self.geometry, start, &run_payload)
                .await;
        }
    }

    async fn complete_mettle_block(&mut self, shared: &mut super::ReceiverShared, block_id: u64) {
        shared.complete_blocks.insert(block_id);
        self.blocks.remove(&block_id);
        if shared.has_all_blocks() {
            shared.mark_object_complete();
            self.report_mettle_complete(shared).await;
        }
    }

    async fn report_mettle_complete(&mut self, shared: &super::ReceiverShared) {
        if self.complete_reported {
            return;
        }
        let round_id = self.last_source_done_round_id.unwrap_or(0);
        let report = NeedReport::Complete;
        self.last_source_done_round_id = Some(round_id);
        self.last_round_need = Some(report.clone());
        shared.send_fec_need(round_id, &report).await;
        self.complete_reported = true;
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
                .unwrap_or_else(|| streaming_mettle_repair_deficit(total));
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
        let max_blocks = max_fec_need_blocks(MAX_FRAMED_PACKET_SIZE, LOSSLESS_TRANSPORT_OVERHEAD)
            .expect("lossless transport must fit a FEC Need frame");
        for block_id in 0..plan.total_blocks() {
            if shared.complete_blocks.contains(&block_id) {
                continue;
            }
            let deficit_symbols = self.block_deficit(shared, block_id);
            if deficit_symbols == 0 {
                warn!(
                    session_id = shared.session_id,
                    local_node_id = shared.local_node_id,
                    block_id,
                    "Lossless FEC block is incomplete but no repair budget remains"
                );
                return None;
            }
            blocks.push(NeedBlock {
                block_id,
                deficit_symbols,
            });
            if blocks.len() == max_blocks {
                break;
            }
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
        let Some(mettle_overhead) = session_fec::mettle_overhead_from_fec_mode(fec_mode) else {
            return;
        };
        for block in blocks {
            let state = self.blocks.entry(block.block_id).or_default();
            if state.mettle.is_none() {
                let Some(mettle) = MettleBlockDecodeState::new(
                    source_symbols,
                    self.geometry.symbol_size(),
                    session_fec::block_seed(shared.session_id, block.block_id),
                    mettle_overhead,
                ) else {
                    continue;
                };
                state.mettle = Some(mettle);
            }
            let _ = block.deficit_symbols;
        }
    }

    pub(super) fn is_complete(&self) -> bool {
        self.complete_reported
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
