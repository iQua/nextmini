use std::collections::BTreeMap;

use nextmini_messages::lossless_session::{
    self, LosslessSessionMode, NeedBlock, NeedReport, max_fec_need_blocks,
};
use tracing::warn;

use crate::node::packet::{LOSSLESS_TRANSPORT_OVERHEAD, MAX_FRAMED_PACKET_SIZE};
use crate::node::session::api::InboundFrame;
use crate::node::session::fec as session_fec;
use crate::node::session::fec::{BlockParams, Decoder};
use crate::node::session::plan::SymbolGeometry;

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
    accepted_symbols: u64,
    source_symbols: u64,
    repair_symbols: u64,
    duplicate_symbols: u64,
    complete_block_symbols: u64,
    invalid_symbols: u64,
    decode_attempts: u64,
    decode_successes: u64,
    decode_insufficient_symbols: u64,
}

#[derive(Debug, Clone, Copy)]
enum DecodeStatus {
    Success,
    InsufficientSymbols,
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

        if fec_mode.scheme_kind().is_none() {
            self.stats.record_invalid(symbol.tree_id);
            return;
        }

        if !self.accept_symbol(shared, &fec_mode, &symbol, payload.to_vec()) {
            return;
        }
        let _ = self
            .try_decode_fec_block(shared, symbol.block_id, &fec_mode)
            .await;
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
        if let Some(block) = systematic_block_payload(
            block_state,
            source_symbols,
            self.geometry.symbol_size(),
            block_len,
        ) {
            self.complete_block(shared, block_id, block).await;
            return true;
        }

        let params = BlockParams::new(
            source_symbols,
            self.geometry.symbol_size(),
            session_fec::block_seed(shared.session_id, block_id),
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
        let Ok(total) = usize::try_from(fec_mode.symbols_per_block) else {
            return 1;
        };
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
            blocks.push(NeedBlock {
                block_id,
                deficit_symbols: self.block_deficit(shared, block_id),
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
        self.last_source_done_round_id = Some(round_id);
        self.last_round_need = Some(report.clone());
        shared.send_fec_need(round_id, &report).await;
        self.complete_reported = matches!(report, NeedReport::Complete);
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
