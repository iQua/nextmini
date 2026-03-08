use std::collections::BTreeMap;

use nextmini_messages::lossless_session::{
    self, BlockStatus, LosslessSessionControl, LosslessSessionMode,
};

use crate::node::session::api::InboundFrame;
use crate::node::session::control;
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
}

impl FecReceiver {
    /// Build receiver-side FEC state from the negotiated symbol geometry.
    pub(super) fn new(geometry: SymbolGeometry) -> Self {
        Self {
            geometry,
            blocks: BTreeMap::new(),
            eot_seen: false,
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
            shared.send_block_ack(symbol.block_id).await;
            return;
        }

        let state = self.blocks.entry(symbol.block_id).or_default();
        let inserted = state
            .symbols
            .insert(symbol.symbol_id, payload.to_vec())
            .is_none();

        if self.try_decode_fec_block(shared, symbol.block_id, &fec_mode).await {
            shared.send_block_ack(symbol.block_id).await;
            return;
        }

        if self.eot_seen && (inserted || !shared.complete_blocks.contains(&symbol.block_id)) {
            self.send_block_status(shared, symbol.block_id).await;
        }
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
            session_fec::block_seed(shared.session.session_id, block_id),
        );
        let decoder = Decoder::from_block(params);
        let mut received = Vec::with_capacity(block_state.symbols.len());

        for (&symbol_id, payload) in &block_state.symbols {
            let mut padded = payload.clone();
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

        let mut block = Vec::with_capacity(output.source_symbols.len() * self.geometry.symbol_size());
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

    /// Report the current deficit for one incomplete FEC block.
    async fn send_block_status(&self, shared: &super::ReceiverShared, block_id: u64) {
        let deficit = self.block_deficit(shared, block_id);
        control::send_control(
            &shared.processors,
            control::FrameRoute {
                session_id: shared.session.session_id,
                tree_id: None,
                src_ip: shared.route.src_ip,
                src_port: shared.route.src_port,
                dst_ip: shared.route.dst_ip,
                dst_port: shared.route.dst_port,
            },
            &LosslessSessionControl::BlockStatus {
                status: BlockStatus {
                    block_id,
                    deficit_symbols: deficit,
                },
            },
        )
        .await;
    }

    /// Emit deficit feedback for every incomplete block after `Eot`.
    pub(super) async fn send_status_for_incomplete_blocks(&self, shared: &super::ReceiverShared) {
        let Some(plan) = shared.plan else {
            return;
        };
        for block_id in 0..plan.total_blocks() {
            if shared.complete_blocks.contains(&block_id) {
                continue;
            }
            self.send_block_status(shared, block_id).await;
        }
    }
}
