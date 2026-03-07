use std::collections::BTreeSet;
use std::net::Ipv4Addr;

use bytes::Bytes;
use tokio::sync::{mpsc, watch};
use tokio::time::{Duration, Instant};
use tracing::{info, warn};

use nextmini_messages::lossless_session::{
    self, BlockStatus, LosslessSessionControl, LosslessSessionManifest, LosslessSessionMode,
};

use crate::node::processor::ProcessorHandle;
use crate::node::scheduler::token_bucket::TokenBucket;
use crate::node::session::api::InboundFrame;
use crate::node::session::control;
use crate::node::session::fec::{self, BlockParams, Encoder};
use crate::node::session::ledger::{BlockState, SessionLedger};
use crate::node::session::plan::{BlockPlan, BlockSpan, SymbolGeometry};
use crate::node::session::runtime::{CommonConfig, SenderConfig};
use crate::node::{NodeId, NodeIdExt};

const MANIFEST_RETRY_INTERVAL: Duration = Duration::from_millis(250);
const IDLE_WAIT: Duration = Duration::from_millis(10);

pub async fn run(
    cfg: SenderConfig,
    mut ctrl_rx: mpsc::Receiver<InboundFrame>,
    processors: ProcessorHandle,
) {
    let mut sender = match SessionSender::new(cfg, processors) {
        Ok(sender) => sender,
        Err(reason) => {
            warn!(reason, "Lossless sender aborted before start");
            return;
        }
    };

    sender.run(&mut ctrl_rx).await;
}

struct SessionSender {
    common: CommonConfig,
    processors: ProcessorHandle,
    manifest: LosslessSessionManifest,
    receiver_ids: Vec<usize>,
    receiver_set: BTreeSet<usize>,
    ready_peers: BTreeSet<usize>,
    plan: BlockPlan,
    source: BlockSource,
    ledger: SessionLedger,
    ready_grace: Duration,
    topology_ready: Option<watch::Receiver<bool>>,
    src_ip: Ipv4Addr,
    pacer: Option<TokenBucket>,
    plain_cursor: u64,
    plain_round_eot_sent: bool,
    fec: Option<FecSenderState>,
}

struct FecSenderState {
    blocks: Vec<FecBlockState>,
    tree_ids: Vec<u16>,
    symbols_per_block: u16,
    geometry: SymbolGeometry,
    next_tree_rr: usize,
    current_source_cache: Option<(u64, Vec<Vec<u8>>)>,
    round_eot_sent: bool,
}

struct FecBlockState {
    next_source_symbol: u32,
    next_fountain_symbol: u32,
    extra_budget: u16,
    encoder: Option<Encoder>,
}

#[derive(Clone)]
struct BlockSource {
    bytes: Bytes,
    total_bytes: u64,
}

impl BlockSource {
    fn new(bytes: Bytes, total_bytes: u64) -> Self {
        Self { bytes, total_bytes }
    }

    fn block_payload(&self, span: BlockSpan) -> Vec<u8> {
        let len = span.len();
        let offset = usize::try_from(span.offset()).ok();
        let full_object_len = usize::try_from(self.total_bytes).ok();

        if let (Some(offset), Some(full_len)) = (offset, full_object_len)
            && self.bytes.len() >= full_len
            && offset + len <= self.bytes.len()
        {
            return self.bytes.slice(offset..offset + len).to_vec();
        }

        if self.bytes.is_empty() {
            return vec![0u8; len];
        }

        let mut out = Vec::with_capacity(len);
        while out.len() < len {
            let remaining = len - out.len();
            let take = remaining.min(self.bytes.len());
            out.extend_from_slice(&self.bytes[..take]);
        }
        out
    }

    fn source_symbols(&self, span: BlockSpan, geometry: SymbolGeometry) -> Vec<Vec<u8>> {
        let block = self.block_payload(span);
        let total_symbol_bytes = geometry.source_symbols() * geometry.symbol_size();
        let mut padded = vec![0u8; total_symbol_bytes];
        let copy_len = block.len().min(total_symbol_bytes);
        padded[..copy_len].copy_from_slice(&block[..copy_len]);
        padded
            .chunks(geometry.symbol_size())
            .map(|chunk| chunk.to_vec())
            .collect()
    }
}

impl SessionSender {
    fn new(cfg: SenderConfig, processors: ProcessorHandle) -> Result<Self, &'static str> {
        let plan = BlockPlan::new(cfg.total_bytes, cfg.common.block_size)
            .map_err(|_| "invalid block plan for sender")?;
        let ledger = SessionLedger::new(plan.total_blocks(), cfg.receiver_ids.iter().copied())
            .map_err(|_| "unable to allocate sender ledger")?;
        let src_ip =
            (cfg.common.local_node_id as NodeId).ip_addr(cfg.common.user_space_base_addr, cfg.common.local_netmask);
        let pacer = cfg.common.data_bucket.clone().map(TokenBucket::new);
        let ready_grace = Duration::from_millis(cfg.ready_grace_ms);
        let source = BlockSource::new(cfg.source_buffer.clone(), cfg.total_bytes);
        let receiver_set = cfg.receiver_ids.iter().copied().collect::<BTreeSet<_>>();
        let fec = build_fec_state(&cfg.manifest, plan)?;

        Ok(Self {
            common: cfg.common,
            processors,
            manifest: cfg.manifest,
            receiver_ids: cfg.receiver_ids,
            receiver_set,
            ready_peers: BTreeSet::new(),
            plan,
            source,
            ledger,
            ready_grace,
            topology_ready: cfg.topology_ready,
            src_ip,
            pacer,
            plain_cursor: 0,
            plain_round_eot_sent: false,
            fec,
        })
    }

    async fn run(&mut self, ctrl_rx: &mut mpsc::Receiver<InboundFrame>) {
        info!(
            session_id = self.common.session_id,
            total_bytes = self.manifest.total_bytes,
            total_blocks = self.manifest.total_blocks,
            receivers = self.receiver_ids.len(),
            fec = self.manifest.mode.is_fec(),
            "Lossless sender started"
        );

        self.wait_topology_ready().await;
        if !self.negotiate_ready(ctrl_rx).await {
            return;
        }

        match &self.manifest.mode {
            LosslessSessionMode::Plain => self.run_plain(ctrl_rx).await,
            LosslessSessionMode::Fec(_) => self.run_fec(ctrl_rx).await,
        }

        info!(
            session_id = self.common.session_id,
            complete = self.ledger.is_complete(),
            "Lossless sender finished"
        );
    }

    async fn wait_topology_ready(&mut self) {
        let Some(rx) = self.topology_ready.as_mut() else {
            return;
        };
        if *rx.borrow() {
            return;
        }

        while rx.changed().await.is_ok() {
            if *rx.borrow() {
                break;
            }
        }
    }

    async fn negotiate_ready(&mut self, ctrl_rx: &mut mpsc::Receiver<InboundFrame>) -> bool {
        if self.receiver_set.is_empty() {
            return true;
        }

        let deadline = Instant::now() + self.ready_grace;
        let mut next_manifest_at = Instant::now();

        while self.ready_peers.len() < self.receiver_set.len() {
            let now = Instant::now();
            if now >= next_manifest_at {
                self.send_manifest().await;
                next_manifest_at = now + MANIFEST_RETRY_INTERVAL;
            }
            if now >= deadline {
                break;
            }

            let wake_at = next_manifest_at.min(deadline);
            tokio::select! {
                maybe_frame = ctrl_rx.recv() => {
                    let Some(frame) = maybe_frame else {
                        return false;
                    };
                    self.handle_control(frame);
                }
                _ = tokio::time::sleep_until(wake_at) => {}
            }
        }

        if self.ready_peers.len() < self.receiver_set.len() {
            let missing = self
                .receiver_set
                .difference(&self.ready_peers)
                .copied()
                .collect::<Vec<_>>();
            warn!(
                session_id = self.common.session_id,
                ?missing,
                "Lossless sender opening data gate before all receivers sent Ready"
            );
        }

        true
    }

    async fn run_plain(&mut self, ctrl_rx: &mut mpsc::Receiver<InboundFrame>) {
        while !self.ledger.is_complete() {
            self.drain_controls(ctrl_rx);

            if let Some(block_id) = self.next_plain_block() {
                self.send_plain_block(block_id).await;
                continue;
            }

            if !self.plain_round_eot_sent {
                self.send_eot().await;
                self.plain_round_eot_sent = true;
                continue;
            }

            if !self.wait_for_signal(ctrl_rx).await {
                break;
            }
            self.plain_cursor = 0;
            self.plain_round_eot_sent = false;
        }
    }

    async fn run_fec(&mut self, ctrl_rx: &mut mpsc::Receiver<InboundFrame>) {
        while !self.ledger.is_complete() {
            self.drain_controls(ctrl_rx);

            if let Some((block_id, symbol_id)) = self.next_fec_source_symbol() {
                if self.send_fec_source_symbol(block_id, symbol_id).await {
                    continue;
                }
                if !self.wait_for_signal(ctrl_rx).await {
                    break;
                }
                continue;
            }

            let round_eot_sent = self.fec.as_ref().map(|state| state.round_eot_sent).unwrap_or(true);
            if !round_eot_sent {
                self.send_eot().await;
                if let Some(fec) = self.fec.as_mut() {
                    fec.round_eot_sent = true;
                }
                continue;
            }

            if let Some((block_id, symbol_id)) = self.next_fec_extra_symbol() {
                if self.send_fec_extra_symbol(block_id, symbol_id).await {
                    continue;
                }
                if !self.wait_for_signal(ctrl_rx).await {
                    break;
                }
                continue;
            }

            if !self.wait_for_signal(ctrl_rx).await {
                break;
            }
        }
    }

    fn drain_controls(&mut self, ctrl_rx: &mut mpsc::Receiver<InboundFrame>) {
        while let Ok(frame) = ctrl_rx.try_recv() {
            self.handle_control(frame);
        }
    }

    async fn wait_for_signal(&mut self, ctrl_rx: &mut mpsc::Receiver<InboundFrame>) -> bool {
        tokio::select! {
            maybe_frame = ctrl_rx.recv() => {
                let Some(frame) = maybe_frame else {
                    return false;
                };
                self.handle_control(frame);
                true
            }
            _ = tokio::time::sleep(IDLE_WAIT) => true,
        }
    }

    fn handle_control(&mut self, frame: InboundFrame) {
        let Some((_, control)) = lossless_session::decode_control(&frame.bytes) else {
            return;
        };

        match control {
            LosslessSessionControl::Manifest { .. } | LosslessSessionControl::Eot => {}
            LosslessSessionControl::Ready { node_id } => {
                if let Ok(node_id) = usize::try_from(node_id)
                    && self.receiver_set.contains(&node_id)
                {
                    self.ready_peers.insert(node_id);
                }
            }
            LosslessSessionControl::BlockAck { block_id } => {
                let Some(peer_id) = frame.peer_id else {
                    return;
                };
                if !self.receiver_set.contains(&peer_id) {
                    return;
                }
                if let Ok(update) = self.ledger.ack_block(peer_id, block_id)
                    && update.block_completed_now
                {
                    self.clear_completed_block(block_id);
                }
            }
            LosslessSessionControl::BlockStatus { status } => {
                let Some(peer_id) = frame.peer_id else {
                    return;
                };
                self.handle_block_status(peer_id, status);
            }
        }
    }

    fn handle_block_status(&mut self, peer_id: usize, status: BlockStatus) {
        if !self.receiver_set.contains(&peer_id) {
            return;
        }
        let Some(fec) = self.fec.as_mut() else {
            return;
        };
        if self
            .ledger
            .receiver_has_acked(peer_id, status.block_id)
            .unwrap_or(false)
        {
            return;
        }
        let Some(block) = fec_block_mut(fec, status.block_id) else {
            return;
        };
        block.extra_budget = block.extra_budget.max(status.deficit_symbols);
    }

    fn clear_completed_block(&mut self, block_id: u64) {
        let Some(fec) = self.fec.as_mut() else {
            return;
        };
        let symbols_per_block = u32::from(fec.symbols_per_block);
        let Some(block) = fec_block_mut(fec, block_id) else {
            return;
        };
        block.extra_budget = 0;
        block.encoder = None;
        block.next_source_symbol = symbols_per_block;
        if let Some((cached_block_id, _)) = &fec.current_source_cache
            && *cached_block_id == block_id
        {
            fec.current_source_cache = None;
        }
    }

    fn next_plain_block(&mut self) -> Option<u64> {
        while self.plain_cursor < self.plan.total_blocks() {
            let block_id = self.plain_cursor;
            self.plain_cursor += 1;
            if self.ledger.block_state(block_id) != Some(BlockState::Complete) {
                self.plain_round_eot_sent = false;
                return Some(block_id);
            }
        }
        None
    }

    fn next_fec_source_symbol(&mut self) -> Option<(u64, u32)> {
        let fec = self.fec.as_mut()?;
        let symbols_per_block = u32::from(fec.symbols_per_block);
        for (block_idx, block) in fec.blocks.iter_mut().enumerate() {
            let block_id = block_idx as u64;
            if self.ledger.block_state(block_id) == Some(BlockState::Complete) {
                continue;
            }
            if block.next_source_symbol < symbols_per_block {
                fec.round_eot_sent = false;
                return Some((block_id, block.next_source_symbol));
            }
        }
        None
    }

    fn next_fec_extra_symbol(&mut self) -> Option<(u64, u32)> {
        let fec = self.fec.as_mut()?;
        for (block_idx, block) in fec.blocks.iter_mut().enumerate() {
            let block_id = block_idx as u64;
            if self.ledger.block_state(block_id) == Some(BlockState::Complete) {
                continue;
            }
            if block.extra_budget > 0 {
                return Some((block_id, block.next_fountain_symbol));
            }
        }
        None
    }

    async fn send_manifest(&mut self) {
        control::send_control(
            &self.processors,
            self.common.session_id,
            self.src_ip,
            self.common.src_port,
            self.common.dest_ip,
            self.common.dst_port,
            &LosslessSessionControl::Manifest {
                manifest: self.manifest.clone(),
            },
        )
        .await;
    }

    async fn send_eot(&mut self) {
        control::send_control(
            &self.processors,
            self.common.session_id,
            self.src_ip,
            self.common.src_port,
            self.common.dest_ip,
            self.common.dst_port,
            &LosslessSessionControl::Eot,
        )
        .await;
    }

    async fn send_plain_block(&mut self, block_id: u64) {
        let Some(span) = self.plan.block_span(block_id) else {
            return;
        };
        let payload = self.source.block_payload(span);
        let frame = lossless_session::encode_block_data(self.common.session_id, block_id, &payload);
        self.pace(frame.len()).await;
        control::send_frame(
            &self.processors,
            self.common.session_id,
            None,
            self.src_ip,
            self.common.src_port,
            self.common.dest_ip,
            self.common.dst_port,
            &frame,
        )
        .await;
    }

    async fn send_fec_source_symbol(&mut self, block_id: u64, symbol_id: u32) -> bool {
        let Some(payload) = self.source_symbol_payload(block_id, symbol_id) else {
            return false;
        };
        self.pace(payload.len()).await;
        if !self.try_send_fec_symbol(block_id, symbol_id, &payload) {
            return false;
        }

        if let Some(block) = self
            .fec
            .as_mut()
            .and_then(|fec| fec_block_mut(fec, block_id))
        {
            block.next_source_symbol += 1;
        }
        true
    }

    async fn send_fec_extra_symbol(&mut self, block_id: u64, symbol_id: u32) -> bool {
        let Some(payload) = self.extra_symbol_payload(block_id, symbol_id) else {
            return false;
        };
        self.pace(payload.len()).await;
        if !self.try_send_fec_symbol(block_id, symbol_id, &payload) {
            return false;
        }

        if let Some(block) = self
            .fec
            .as_mut()
            .and_then(|fec| fec_block_mut(fec, block_id))
        {
            block.extra_budget = block.extra_budget.saturating_sub(1);
            block.next_fountain_symbol += 1;
        }
        true
    }

    fn try_send_fec_symbol(&mut self, block_id: u64, symbol_id: u32, payload: &[u8]) -> bool {
        let Some(fec) = self.fec.as_mut() else {
            return false;
        };

        for offset in 0..fec.tree_ids.len() {
            let idx = (fec.next_tree_rr + offset) % fec.tree_ids.len();
            let tree_id = fec.tree_ids[idx];
            let frame = lossless_session::encode_block_symbol(
                self.common.session_id,
                block_id,
                symbol_id,
                tree_id,
                payload,
            );
            let submission = control::try_send_frame(
                &self.processors,
                self.common.session_id,
                Some(tree_id),
                self.src_ip,
                self.common.src_port,
                self.common.dest_ip,
                self.common.dst_port,
                &frame,
            );
            match submission.outcome {
                crate::node::processor::SendOutcome::Queued => {
                    fec.next_tree_rr = (idx + 1) % fec.tree_ids.len();
                    return true;
                }
                crate::node::processor::SendOutcome::WouldBlock => {}
                crate::node::processor::SendOutcome::Closed => {
                    warn!(
                        session_id = self.common.session_id,
                        tree_id,
                        "Lossless sender observed closed processor ingress while sending FEC symbol"
                    );
                }
            }
        }

        false
    }

    fn source_symbol_payload(&mut self, block_id: u64, symbol_id: u32) -> Option<Vec<u8>> {
        let fec = self.fec.as_mut()?;
        ensure_source_symbol_cache(&self.source, self.plan, fec, block_id)?;
        let (_, symbols) = fec.current_source_cache.as_ref()?;
        let idx = usize::try_from(symbol_id).ok()?;
        symbols.get(idx).cloned()
    }

    fn extra_symbol_payload(&mut self, block_id: u64, symbol_id: u32) -> Option<Vec<u8>> {
        let Some(fec_state) = self.fec.as_ref() else {
            return None;
        };
        let geometry = fec_state.geometry;
        let symbols_per_block = fec_state.symbols_per_block;

        let need_encoder = self
            .fec
            .as_ref()
            .and_then(|fec| fec_block_ref(fec, block_id))
            .map(|block| block.encoder.is_none())?;

        if need_encoder {
            let span = self.plan.block_span(block_id)?;
            let source_symbols = self.source.source_symbols(span, geometry);
            let params = BlockParams::new(
                usize::from(symbols_per_block),
                geometry.symbol_size(),
                fec::block_seed(self.common.session_id, block_id),
            );
            let block = self
                .fec
                .as_mut()
                .and_then(|fec| fec_block_mut(fec, block_id))?;
            if block.encoder.is_none() {
                block.encoder = Encoder::from_block(params, &source_symbols);
            }
        }

        self.fec
            .as_ref()
            .and_then(|fec| fec_block_ref(fec, block_id))
            .and_then(|block| block.encoder.as_ref())
            .map(|encoder| encoder.coded_symbol(symbol_id))
    }

    async fn pace(&mut self, bytes: usize) {
        if let Some(bucket) = self.pacer.as_mut() {
            bucket.wait_for_bytes(bytes).await;
        }
    }
}

fn build_fec_state(
    manifest: &LosslessSessionManifest,
    plan: BlockPlan,
) -> Result<Option<FecSenderState>, &'static str> {
    let LosslessSessionMode::Fec(fec) = &manifest.mode else {
        return Ok(None);
    };
    let geometry = plan
        .symbol_geometry(fec.symbols_per_block)
        .map_err(|_| "invalid symbol geometry for fec sender")?;
    let block_count = usize::try_from(plan.total_blocks()).map_err(|_| "too many blocks for fec sender")?;

    Ok(Some(FecSenderState {
        blocks: (0..block_count)
            .map(|_| FecBlockState {
                next_source_symbol: 0,
                next_fountain_symbol: u32::from(fec.symbols_per_block),
                extra_budget: 0,
                encoder: None,
            })
            .collect(),
        tree_ids: fec.tree_ids.clone(),
        symbols_per_block: fec.symbols_per_block,
        geometry,
        next_tree_rr: 0,
        current_source_cache: None,
        round_eot_sent: false,
    }))
}

fn ensure_source_symbol_cache(
    source: &BlockSource,
    plan: BlockPlan,
    fec: &mut FecSenderState,
    block_id: u64,
) -> Option<()> {
    let needs_refresh = !matches!(&fec.current_source_cache, Some((cached_block_id, _)) if *cached_block_id == block_id);
    if !needs_refresh {
        return Some(());
    }

    let span = plan.block_span(block_id)?;
    let symbols = source.source_symbols(span, fec.geometry);
    fec.current_source_cache = Some((block_id, symbols));
    Some(())
}

fn fec_block_mut(fec: &mut FecSenderState, block_id: u64) -> Option<&mut FecBlockState> {
    let idx = usize::try_from(block_id).ok()?;
    fec.blocks.get_mut(idx)
}

fn fec_block_ref(fec: &FecSenderState, block_id: u64) -> Option<&FecBlockState> {
    let idx = usize::try_from(block_id).ok()?;
    fec.blocks.get(idx)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn block_source_repeats_template_when_buffer_is_short() {
        let source = BlockSource::new(Bytes::from_static(b"ab"), 8);
        let plan = BlockPlan::new(8, 4).expect("valid plan");
        let payload = source.block_payload(plan.block_span(1).expect("second block"));
        assert_eq!(payload, b"abab");
    }

    #[test]
    fn block_source_builds_fixed_size_source_symbols() {
        let source = BlockSource::new(Bytes::from_static(b"abcdef"), 6);
        let plan = BlockPlan::new(6, 6).expect("valid plan");
        let geometry = plan.symbol_geometry(4).expect("valid geometry");
        let symbols = source.source_symbols(plan.block_span(0).expect("first block"), geometry);

        assert_eq!(symbols.len(), 4);
        assert_eq!(symbols[0], b"ab");
        assert_eq!(symbols[1], b"cd");
        assert_eq!(symbols[2], b"ef");
        assert_eq!(symbols[3], b"\0\0");
    }
}
