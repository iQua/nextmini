use std::collections::BTreeMap;

use nextmini_messages::lossless_session::{self, BlockStatus, FecStatus, LosslessSessionMode};
use tracing::warn;

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
    pub(super) eot_seen: bool,
    complete_reported: bool,
}

impl FecReceiver {
    /// Build receiver-side FEC state from the negotiated symbol geometry.
    pub(super) fn new(geometry: SymbolGeometry) -> Self {
        Self {
            geometry,
            blocks: BTreeMap::new(),
            eot_seen: false,
            complete_reported: false,
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
            return;
        }
        if shared.complete_blocks.contains(&symbol.block_id) {
            return;
        }

        let state = self.blocks.entry(symbol.block_id).or_default();
        state.symbols.insert(symbol.symbol_id, payload.to_vec());

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
        if block_state.symbols.len() < usize::from(fec_mode.symbols_per_block) {
            return false;
        }

        let params = BlockParams::new(
            usize::from(fec_mode.symbols_per_block),
            self.geometry.symbol_size(),
            session_fec::block_seed(shared.session_id, block_id),
        );
        let decoder = Decoder::from_block(params);
        let mut received = Vec::with_capacity(block_state.symbols.len());

        for (&symbol_id, payload) in &block_state.symbols {
            let mut padded = payload.clone();
            let additional = self.geometry.symbol_size().saturating_sub(padded.len());
            if padded.try_reserve_exact(additional).is_err() {
                warn!(
                    session_id = shared.session_id,
                    block_id,
                    symbol_size = self.geometry.symbol_size(),
                    "Lossless receiver failed to reserve space for FEC symbol padding"
                );
                return false;
            }
            padded.resize(self.geometry.symbol_size(), 0);
            if symbol_id < u32::from(fec_mode.symbols_per_block) {
                received.push(decoder.source_symbol(symbol_id, padded));
            } else {
                received.push(decoder.coded_symbol(symbol_id, padded));
            }
        }

        let Ok(output) = decoder.decode(&received) else {
            return false;
        };
        let Some(block_len) = plan.block_len(block_id) else {
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

        shared.write_block(block_id, &block).await;
        shared.complete_blocks.insert(block_id);
        self.blocks.remove(&block_id);
        true
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
            .map(|state| state.symbols.len())
            .unwrap_or(0);
        let total = usize::from(fec_mode.symbols_per_block);
        if present >= total {
            1
        } else {
            u16::try_from(total - present).unwrap_or(u16::MAX).max(1)
        }
    }

    pub(super) fn status(&self, shared: &super::ReceiverShared) -> Option<FecStatus> {
        let Some(plan) = shared.plan else {
            return None;
        };
        if plan.total_blocks() == 0 || shared.has_all_blocks() {
            return Some(FecStatus::Complete);
        };
        let mut blocks = Vec::new();
        for block_id in 0..plan.total_blocks() {
            if shared.complete_blocks.contains(&block_id) {
                continue;
            }
            blocks.push(BlockStatus {
                block_id,
                deficit_symbols: self.block_deficit(shared, block_id),
            });
        }
        Some(FecStatus::MissingBlocks { blocks })
    }

    pub(super) async fn handle_eot(&mut self, shared: &super::ReceiverShared) {
        self.eot_seen = true;
        let Some(status) = self.status(shared) else {
            return;
        };
        shared.send_fec_status(&status).await;
        self.complete_reported = matches!(status, FecStatus::Complete);
    }

    pub(super) fn is_complete(&self) -> bool {
        self.complete_reported
    }
}
