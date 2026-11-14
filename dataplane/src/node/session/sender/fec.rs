use bytes::Bytes;
use std::collections::BTreeMap;
use tokio::sync::mpsc;
use tokio::time::{Duration, Instant};
use tracing::{debug, info, warn};

use nextmini_messages::lossless_session::{
    LosslessSessionManifest, LosslessSessionMode, NeedBlock, NeedReport,
};

use crate::node::processor::{LosslessIngressContract, SendOutcome};
use crate::node::session::api::InboundFrame;
use crate::node::session::api::SessionOutcome;
use crate::node::session::control;
use crate::node::session::fec as session_fec;
use crate::node::session::fec::{BlockParams, Encoder};
use crate::node::session::plan::{BlockPlan, SymbolGeometry};

use super::block_symbol_frame;

const FEC_SEND_STATS_LOG_MASK: u64 = (1 << 12) - 1;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RoundPhase {
    SendingData,
    WaitingForReports,
}

/// FEC-mode sender state and scheduling cursors.
pub(super) struct FecSender {
    blocks: Vec<FecBlockState>,
    /// Index of the first block that may still have unsent source symbols.
    ///
    /// Invariant: every block before it has `next_source_symbol >= symbols_per_block`.
    source_cursor: usize,
    symbols_per_block: u32,
    tree_ids: Vec<u16>,
    symbol_retry_sweep_width: usize,
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
    ingress_contract_checked: bool,
    stats: FecSenderStats,
}

/// Per-block sender cursor and encoder state for FEC mode.
struct FecBlockState {
    next_source_symbol: u32,
    next_fountain_symbol: u32,
    required_extra_symbols: u32,
    emitted_extra_symbols: u32,
    encoder: Option<Encoder>,
}

#[derive(Debug)]
struct FecSenderStats {
    queued: u64,
    events: u64,
    would_block_by_tree: Box<[u64]>,
    cumulative_stall: Duration,
}

impl FecSenderStats {
    fn new(tree_count: usize) -> Self {
        Self {
            queued: 0,
            events: 0,
            would_block_by_tree: vec![0; tree_count].into_boxed_slice(),
            cumulative_stall: Duration::ZERO,
        }
    }

    fn record_queued(&mut self) -> bool {
        self.queued = self.queued.saturating_add(1);
        self.record_event()
    }

    fn record_would_block(&mut self, tree_idx: usize) -> bool {
        if let Some(count) = self.would_block_by_tree.get_mut(tree_idx) {
            *count = count.saturating_add(1);
        }
        self.record_event()
    }

    fn record_stall(&mut self, duration: Duration) {
        self.cumulative_stall = self.cumulative_stall.saturating_add(duration);
    }

    fn record_event(&mut self) -> bool {
        self.events = self.events.saturating_add(1);
        self.events & FEC_SEND_STATS_LOG_MASK == 0
    }

    fn log(&self, session_id: u64, tree_ids: &[u16]) {
        debug!(
            session_id,
            queued = self.queued,
            tree_ids = ?tree_ids,
            would_block_by_tree = ?self.would_block_by_tree,
            cumulative_stall_ms = self.cumulative_stall.as_millis(),
            "Lossless RaptorQ sender ingress statistics"
        );
    }

    fn total_queued(&self) -> u64 {
        self.queued
    }
}

impl FecSender {
    /// Build the initial FEC sender state for a validated manifest.
    ///
    /// FEC symbols are striped across `manifest.fec.tree_ids` in flat
    /// round-robin order. The sender does not consume solver-derived tree
    /// weights.
    pub(super) fn new(
        manifest: &LosslessSessionManifest,
        plan: BlockPlan,
    ) -> Result<Self, &'static str> {
        let LosslessSessionMode::Fec(fec) = &manifest.mode else {
            return Err("attempted to build fec sender for plain manifest");
        };
        if fec.scheme_kind().is_none() {
            return Err("unsupported fec scheme for fec sender");
        }
        let geometry = plan
            .symbol_geometry(fec.symbols_per_block)
            .map_err(|_| "invalid symbol geometry for fec sender")?;
        let block_count =
            usize::try_from(plan.total_blocks()).map_err(|_| "too many blocks for fec sender")?;
        let tree_ids = fec.tree_ids.clone();
        let symbol_retry_sweep_width = tree_ids.len();
        let stats = FecSenderStats::new(tree_ids.len());
        Ok(Self {
            blocks: (0..block_count)
                .map(|_| FecBlockState {
                    next_source_symbol: 0,
                    next_fountain_symbol: fec.symbols_per_block,
                    required_extra_symbols: 0,
                    emitted_extra_symbols: 0,
                    encoder: None,
                })
                .collect(),
            source_cursor: 0,
            symbols_per_block: fec.symbols_per_block,
            tree_ids,
            symbol_retry_sweep_width,
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
            ingress_contract_checked: false,
            stats,
        })
    }

    /// Main send loop for FEC mode.
    ///
    /// RaptorQ sends raw source symbols before feedback-driven repairs.
    pub(super) async fn run(
        &mut self,
        shared: &mut super::SenderShared,
        ctrl_rx: &mut mpsc::Receiver<InboundFrame>,
    ) -> SessionOutcome {
        self.apply_ingress_contract(shared);

        while !self.is_complete() {
            shared.drain_controls(ctrl_rx, self);
            if self.protocol_error {
                return SessionOutcome::Aborted;
            }

            if let Some((block_id, symbol_id)) = self.next_source_symbol() {
                if !self.send_source_symbol(shared, block_id, symbol_id).await {
                    return SessionOutcome::Aborted;
                }
                self.round_source_done_sent = false;
                continue;
            }

            if let Some((block_id, symbol_id)) = self.next_extra_symbol() {
                if !self.send_extra_symbol(shared, block_id, symbol_id).await {
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
                    return SessionOutcome::Aborted;
                }
            }
        }

        SessionOutcome::Completed
    }

    fn apply_ingress_contract(&mut self, shared: &super::SenderShared) {
        if self.ingress_contract_checked {
            return;
        }
        self.ingress_contract_checked = true;

        if self.tree_ids.len() <= 1 {
            return;
        }

        let tree_id = self.tree_ids[0];
        let packet = control::build_packet(
            control::FrameRoute {
                session_id: shared.session.session_id,
                tree_id: Some(tree_id),
                src_ip: shared.route.src_ip,
                src_port: shared.route.src_port,
                dst_ip: shared.route.dst_ip,
                dst_port: shared.route.dst_port,
            },
            &[],
        );
        if shared.processors.lossless_ingress_contract(&packet)
            == LosslessIngressContract::SharedQueueNonBlocking
        {
            self.symbol_retry_sweep_width = 1;
            warn!(
                session_id = shared.session.session_id,
                tree_count = self.tree_ids.len(),
                "Lossless RaptorQ sender detected shared ingress; per-symbol tree failover disabled while striping is retained"
            );
        }
    }

    pub(super) fn is_complete(&self) -> bool {
        self.round_complete
    }

    /// Return the next source symbol to send in FEC mode.
    ///
    /// This is the first block whose `next_source_symbol` is below the source-phase limit, found
    /// from `source_cursor` instead of rescanning every block per symbol. That is equivalent to a
    /// full scan because, for the lifetime of the sender:
    /// - `next_source_symbol` only ever increases (it starts at 0 and is bumped by one in
    ///   `send_source_symbol`; finishing a report round resets only the extra-symbol counters), so
    ///   a block that has reached the limit never becomes sendable again;
    /// - the source-phase limit `symbols_per_block` is fixed at construction;
    /// - `blocks` is never resized or replaced.
    ///
    /// Later rounds flip `phase` back to `SendingData` without resetting source counters, so the
    /// cursor stays at the end and source scheduling correctly yields nothing. Anything that ever
    /// lowers a counter or the limit must also reset `source_cursor` to 0.
    fn next_source_symbol(&mut self) -> Option<(u64, u32)> {
        if self.phase != RoundPhase::SendingData {
            return None;
        }
        while let Some(block) = self.blocks.get(self.source_cursor) {
            if block.next_source_symbol < self.symbols_per_block {
                return Some((self.source_cursor as u64, block.next_source_symbol));
            }
            self.source_cursor += 1;
        }
        None
    }

    /// Return the next extra fountain symbol requested by a receiver.
    fn next_extra_symbol(&mut self) -> Option<(u64, u32)> {
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
        if !self
            .send_symbol(shared, block_id, symbol_id, payload.as_ref())
            .await
        {
            return false;
        }

        if let Some(block) = fec_block_mut(self, block_id) {
            let Some(next_symbol) = block.next_source_symbol.checked_add(1) else {
                self.protocol_error = true;
                return false;
            };
            block.next_source_symbol = next_symbol;
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
            self.protocol_error = true;
            return false;
        };
        shared.pace(payload.len()).await;
        if !self
            .send_symbol(shared, block_id, symbol_id, &payload)
            .await
        {
            return false;
        }

        if let Some(block) = fec_block_mut(self, block_id) {
            block.emitted_extra_symbols = block.emitted_extra_symbols.saturating_add(1);
            block.next_fountain_symbol += 1;
        }
        true
    }

    /// Emit one FEC symbol through the processor ingress path.
    async fn send_symbol(
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
        let sweep_width = self.symbol_retry_sweep_width.clamp(1, tree_count);
        let mut stall_started: Option<Instant> = None;
        loop {
            let start_idx = self.next_tree_rr % tree_count;
            let initial_tree_id = self.tree_ids[start_idx];
            block_symbol_frame::encode_into(
                &mut self.frame_scratch,
                shared.session.session_id,
                block_id,
                symbol_id,
                initial_tree_id,
                payload,
            );

            let mut saw_would_block = false;
            let mut frame_tree_id = initial_tree_id;
            for offset in 0..sweep_width {
                let idx = (start_idx + offset) % tree_count;
                let tree_id = self.tree_ids[idx];
                if tree_id != frame_tree_id {
                    block_symbol_frame::patch_tree_id(&mut self.frame_scratch, tree_id)
                        .expect("encoded block symbol should accept tree-id patch");
                    frame_tree_id = tree_id;
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
                        if let Some(stall_started) = stall_started.take() {
                            self.stats.record_stall(stall_started.elapsed());
                        }
                        if self.stats.record_queued() {
                            self.stats.log(shared.session.session_id, &self.tree_ids);
                        }
                        self.note_queued_symbol(
                            shared, block_id, symbol_id, tree_id, idx, tree_count,
                        );
                        return true;
                    }
                    SendOutcome::WouldBlock => {
                        saw_would_block = true;
                        stall_started.get_or_insert_with(Instant::now);
                        if self.stats.record_would_block(idx) {
                            self.stats.log(shared.session.session_id, &self.tree_ids);
                        }
                    }
                    SendOutcome::Closed => {
                        warn!(
                            session_id = shared.session.session_id,
                            tree_id,
                            "Lossless sender observed closed processor ingress while sending FEC symbol"
                        );
                    }
                }
            }

            if !saw_would_block {
                return false;
            }

            tokio::task::yield_now().await;
        }
    }

    fn note_queued_symbol(
        &mut self,
        shared: &super::SenderShared,
        block_id: u64,
        symbol_id: u32,
        tree_id: u16,
        idx: usize,
        tree_count: usize,
    ) {
        if self.stats.total_queued() == 1 {
            info!(
                session_id = shared.session.session_id,
                tree_id, block_id, symbol_id, "Lossless sender queued first FEC payload symbol"
            );
        }
        self.next_tree_rr = (idx + 1) % tree_count;
    }

    /// Return the next raw source-symbol payload for one block.
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

    /// Lazily build an encoder and derive one coded/fountain symbol payload.
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
                usize::try_from(self.symbols_per_block).ok()?,
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
            .and_then(|encoder| encoder.coded_symbol(symbol_id))
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
        let mut all_complete = true;
        let reports: Vec<_> = self.round_reports.values().cloned().collect();

        for status in reports {
            match status {
                NeedReport::Complete => {}
                NeedReport::Fec { blocks } => {
                    all_complete = false;
                    self.record_requested_repairs(&blocks);
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
                self.record_requested_repairs(&blocks);
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
            .map(|block| block.required_extra_symbols)
            .sum()
    }

    fn total_emitted_extra_symbols(&self) -> u32 {
        self.blocks
            .iter()
            .map(|block| block.emitted_extra_symbols)
            .sum()
    }

    fn record_requested_repairs(&mut self, blocks: &[NeedBlock]) {
        for block in blocks {
            if let Some(entry) = self
                .blocks
                .get_mut(usize::try_from(block.block_id).ok().unwrap_or(usize::MAX))
            {
                entry.required_extra_symbols = entry
                    .required_extra_symbols
                    .max(u32::from(block.deficit_symbols));
            }
        }
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
    use crate::node::processor::{ProcessorHandle, ProcessorPacket};
    use crate::node::session::plan::BlockPlan;
    use crate::node::session::runtime::{SessionConfig, TransportRoute};
    use crate::node::session::sender::state::{ActiveSessionQuorum, QuorumLiveness};
    use crate::node::session::sender::{BlockSource, ModeHooks, SenderShared};
    use nextmini_messages::OperatingMode;
    use nextmini_messages::lossless_session::{LosslessSessionFecMode, NeedBlock};

    #[tokio::test]
    async fn fec_tree_ids_drive_round_robin_order() {
        let manifest = LosslessSessionManifest {
            block_size: 16,
            total_bytes: 16,
            total_blocks: 1,
            mode: LosslessSessionMode::Fec(LosslessSessionFecMode::new_raptorq(4, vec![7, 9, 11])),
        };
        let plan = BlockPlan::new(16, 16).expect("valid plan");
        let mut sender = FecSender::new(&manifest, plan).expect("sender should build");
        let mut shared = test_sender_shared(manifest);
        let (processors, mut processor_packets) =
            ProcessorHandle::new_sequential_stub_for_test(LocalConfig {
                node_id: 0,
                n_nodes: 3,
                channel_capacity: 8,
                operating_mode: OperatingMode::Normal,
                user_space_base_addr: Ipv4Addr::new(10, 0, 0, 0),
                local_netmask: Ipv4Addr::new(255, 255, 255, 0),
                ..Default::default()
            });
        shared.processors = processors;

        for symbol_id in 0..6 {
            assert!(
                sender.send_symbol(&mut shared, 0, symbol_id, b"data").await,
                "symbol should enter the stub processor"
            );
        }

        let mut observed = Vec::new();
        for expected_symbol_id in 0..6 {
            let ProcessorPacket::ProcessPacket(packet) = processor_packets
                .recv()
                .await
                .expect("stub processor channel closed")
            else {
                panic!("stub processor received an unexpected link-probe message");
            };
            let route_tree_id = packet
                .lossless_fec_tree_id()
                .expect("FEC packet should carry a route tree id");
            let (_, symbol, _) = nextmini_messages::lossless_session::decode_block_symbol(
                packet
                    .tcp_payload()
                    .expect("FEC packet should carry a block symbol frame"),
            )
            .expect("FEC packet payload should decode as a block symbol");
            assert_eq!(symbol.symbol_id, expected_symbol_id);
            assert_eq!(symbol.tree_id, route_tree_id);
            observed.push(route_tree_id);
        }

        assert_eq!(observed, vec![7, 9, 11, 7, 9, 11]);
    }

    #[tokio::test]
    async fn fec_sender_run_retains_tree_striping_on_shared_ingress() {
        let manifest = LosslessSessionManifest {
            block_size: 16,
            total_bytes: 16,
            total_blocks: 1,
            mode: LosslessSessionMode::Fec(LosslessSessionFecMode::new_raptorq(4, vec![7, 9, 11])),
        };
        let plan = BlockPlan::new(16, 16).expect("valid plan");
        let mut sender = FecSender::new(&manifest, plan).expect("sender should build");
        let mut shared = test_sender_shared(manifest);
        shared.processors = ProcessorHandle::new(LocalConfig {
            node_id: 0,
            n_nodes: 1,
            num_packet_processors: 1,
            channel_capacity: 8,
            operating_mode: OperatingMode::Max,
            user_space_base_addr: Ipv4Addr::new(10, 0, 0, 0),
            local_netmask: Ipv4Addr::new(255, 255, 255, 0),
            ..Default::default()
        });
        shared.active_quorum = ActiveSessionQuorum::new([]);
        shared.active_quorum.freeze();
        let (_ctrl_tx, mut ctrl_rx) = mpsc::channel(1);

        let outcome = sender.run(&mut shared, &mut ctrl_rx).await;

        assert_eq!(outcome, SessionOutcome::Completed);
        assert!(sender.ingress_contract_checked);
        assert_eq!(sender.tree_ids, vec![7, 9, 11]);
        assert_eq!(sender.symbol_retry_sweep_width, 1);
        assert_eq!(
            sender.next_tree_rr,
            sender.symbols_per_block as usize % sender.tree_ids.len()
        );
    }

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
            sender.next_extra_symbol(),
            Some((0, sender.symbols_per_block)),
            "first quorum report should open a bounded speculative repair window"
        );
        if let Some(block) = sender.blocks.first_mut() {
            block.emitted_extra_symbols = 2;
            block.next_fountain_symbol = sender.symbols_per_block + 2;
        }
        assert_eq!(
            sender.next_extra_symbol(),
            None,
            "speculative repair should stop once the partial window is exhausted"
        );

        sender.on_need(&mut shared, 23, 0, NeedReport::Complete);

        assert_eq!(
            sender.next_extra_symbol(),
            Some((0, sender.symbols_per_block + 2)),
            "full quorum should open the remaining repair budget"
        );
    }

    /// The original full rescan of every block, kept as the reference ordering.
    fn reference_next_source_symbol(sender: &FecSender) -> Option<(u64, u32)> {
        let limit = sender.symbols_per_block;
        for (block_idx, block) in sender.blocks.iter().enumerate() {
            if block.next_source_symbol < limit && sender.phase == RoundPhase::SendingData {
                return Some((block_idx as u64, block.next_source_symbol));
            }
        }
        None
    }

    fn assert_source_order_matches_full_scan(mode: LosslessSessionFecMode) {
        let total_blocks = 48u64;
        let manifest = LosslessSessionManifest {
            block_size: 64,
            total_bytes: 64 * total_blocks,
            total_blocks,
            mode: LosslessSessionMode::Fec(mode),
        };
        let plan = BlockPlan::new(manifest.total_bytes, 64).expect("valid plan");
        let mut sender = FecSender::new(&manifest, plan).expect("FEC sender");
        let limit = sender.symbols_per_block;

        let mut sent = Vec::new();
        while let Some(next) = sender.next_source_symbol() {
            assert_eq!(Some(next), reference_next_source_symbol(&sender));
            sent.push(next);
            // Mirror send_source_symbol's counter bump.
            sender.blocks[next.0 as usize].next_source_symbol += 1;
        }
        assert_eq!(reference_next_source_symbol(&sender), None);
        assert_eq!(sent.len() as u64, total_blocks * u64::from(limit));
        // Blocks are drained in order, one full block at a time.
        assert!(sent.windows(2).all(|pair| pair[0] <= pair[1]));

        // Later rounds return to SendingData without resetting source counters.
        sender.phase = RoundPhase::WaitingForReports;
        assert_eq!(sender.next_source_symbol(), None);
        sender.phase = RoundPhase::SendingData;
        assert_eq!(sender.next_source_symbol(), None);
    }

    #[test]
    fn source_cursor_preserves_raptorq_many_block_send_order() {
        assert_source_order_matches_full_scan(LosslessSessionFecMode::new_raptorq(4, vec![7, 9]));
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
        let source_buffer = Bytes::from_static(b"abcdefghijklmnop");
        let processors = ProcessorHandle::new(LocalConfig {
            node_id: 0,
            n_nodes: 1,
            num_packet_processors: 1,
            channel_capacity: 8,
            user_space_base_addr: Ipv4Addr::new(10, 0, 0, 0),
            local_netmask: Ipv4Addr::new(255, 255, 255, 0),
            ..Default::default()
        });
        let block_size = usize::try_from(manifest.block_size).expect("test block size fits usize");
        let plan = BlockPlan::new(manifest.total_bytes, block_size).expect("valid plan");

        SenderShared {
            session: SessionConfig {
                session_id: 7,
                block_size,
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
            plan,
            source: BlockSource::new(source_buffer.clone(), source_buffer.len() as u64),
            ready_grace: Duration::from_millis(1),
            topology_ready: None,
            pacer: None,
        }
    }
}
