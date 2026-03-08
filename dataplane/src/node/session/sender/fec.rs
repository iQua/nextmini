use tokio::sync::mpsc;
use tracing::warn;

use nextmini_messages::lossless_session::{
    self, BlockStatus, LosslessSessionManifest, LosslessSessionMode,
};

use crate::node::processor::SendOutcome;
use crate::node::session::api::InboundFrame;
use crate::node::session::control;
use crate::node::session::fec as session_fec;
use crate::node::session::fec::{BlockParams, Encoder};
use crate::node::session::ledger::BlockState;
use crate::node::session::plan::{BlockPlan, SymbolGeometry};

/// FEC-mode sender state and scheduling cursors.
pub(super) struct FecSender {
    blocks: Vec<FecBlockState>,
    symbols_per_block: u16,
    tree_ids: Vec<u16>,
    geometry: SymbolGeometry,
    next_tree_rr: usize,
    current_source_cache: Option<(u64, Vec<Vec<u8>>)>,
    round_eot_sent: bool,
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
            round_eot_sent: false,
        })
    }

    /// Main send loop for FEC mode.
    ///
    /// Source symbols are always sent before extra fountain symbols. The sender
    /// only emits extra symbols after it has completed a source-symbol sweep
    /// and received per-block deficit feedback.
    pub(super) async fn run(
        &mut self,
        shared: &mut super::SenderShared,
        ctrl_rx: &mut mpsc::Receiver<InboundFrame>,
    ) {
        while !shared.ledger.is_complete() {
            shared.drain_controls(ctrl_rx, self);

            if let Some((block_id, symbol_id)) = self.next_source_symbol(shared) {
                if self.send_source_symbol(shared, block_id, symbol_id).await {
                    continue;
                }
                if !shared.wait_for_signal(ctrl_rx, self).await {
                    break;
                }
                continue;
            }

            if !self.round_eot_sent {
                shared.send_eot().await;
                self.round_eot_sent = true;
                continue;
            }

            if let Some((block_id, symbol_id)) = self.next_extra_symbol(shared) {
                if self.send_extra_symbol(shared, block_id, symbol_id).await {
                    continue;
                }
                if !shared.wait_for_signal(ctrl_rx, self).await {
                    break;
                }
                continue;
            }

            if !shared.wait_for_signal(ctrl_rx, self).await {
                break;
            }
        }
    }

    /// Return the next source symbol to send in FEC mode.
    fn next_source_symbol(&mut self, shared: &super::SenderShared) -> Option<(u64, u32)> {
        let symbols_per_block = u32::from(self.symbols_per_block);
        for (block_idx, block) in self.blocks.iter_mut().enumerate() {
            let block_id = block_idx as u64;
            if shared.ledger.block_state(block_id) == Some(BlockState::Complete) {
                continue;
            }
            if block.next_source_symbol < symbols_per_block {
                self.round_eot_sent = false;
                return Some((block_id, block.next_source_symbol));
            }
        }
        None
    }

    /// Return the next extra fountain symbol requested by a receiver.
    fn next_extra_symbol(&mut self, shared: &super::SenderShared) -> Option<(u64, u32)> {
        for (block_idx, block) in self.blocks.iter_mut().enumerate() {
            let block_id = block_idx as u64;
            if shared.ledger.block_state(block_id) == Some(BlockState::Complete) {
                continue;
            }
            if block.extra_budget > 0 {
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
        if !self.try_send_symbol(shared, block_id, symbol_id, &payload) {
            return false;
        }

        if let Some(block) = fec_block_mut(self, block_id) {
            block.next_source_symbol += 1;
        }
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
        for offset in 0..self.tree_ids.len() {
            let idx = (self.next_tree_rr + offset) % self.tree_ids.len();
            let tree_id = self.tree_ids[idx];
            let frame = lossless_session::encode_block_symbol(
                shared.session.session_id,
                block_id,
                symbol_id,
                tree_id,
                payload,
            );
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
                &frame,
            );
            match submission.outcome {
                SendOutcome::Queued => {
                    self.next_tree_rr = (idx + 1) % self.tree_ids.len();
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
    ) -> Option<Vec<u8>> {
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
        let need_encoder = fec_block_ref(self, block_id)
            .map(|block| block.encoder.is_none())?;

        if need_encoder {
            let span = shared.plan.block_span(block_id)?;
            let source_symbols = shared.source.source_symbols(span, self.geometry);
            let params = BlockParams::new(
                usize::from(self.symbols_per_block),
                self.geometry.symbol_size(),
                session_fec::block_seed(shared.session.session_id, block_id),
            );
            let block = fec_block_mut(self, block_id)?;
            if block.encoder.is_none() {
                block.encoder = Encoder::from_block(params, &source_symbols);
            }
        }

        fec_block_ref(self, block_id)
            .and_then(|block| block.encoder.as_ref())
            .map(|encoder| encoder.coded_symbol(symbol_id))
    }
}

impl super::ModeHooks for FecSender {
    /// Record additional symbol demand for one FEC block.
    fn on_block_status(&mut self, shared: &super::SenderShared, peer_id: usize, status: BlockStatus) {
        if !shared.receiver_set.contains(&peer_id) {
            return;
        }
        if shared
            .ledger
            .receiver_has_acked(peer_id, status.block_id)
            .unwrap_or(false)
        {
            return;
        }
        let Some(block) = fec_block_mut(self, status.block_id) else {
            return;
        };
        block.extra_budget = block.extra_budget.max(status.deficit_symbols);
    }

    /// Drop encoder/cache state once a block is fully acknowledged.
    fn on_block_completed(&mut self, block_id: u64) {
        let symbols_per_block = u32::from(self.symbols_per_block);
        let Some(block) = fec_block_mut(self, block_id) else {
            return;
        };
        block.extra_budget = 0;
        block.encoder = None;
        block.next_source_symbol = symbols_per_block;
        if let Some((cached_block_id, _)) = &self.current_source_cache
            && *cached_block_id == block_id
        {
            self.current_source_cache = None;
        }
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
    let symbols = source.source_symbols(span, fec.geometry);
    fec.current_source_cache = Some((block_id, symbols));
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
