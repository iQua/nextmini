use std::collections::{BTreeMap, BTreeSet, HashSet, VecDeque};
use std::net::Ipv4Addr;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use bytes::{Bytes, BytesMut};
use tokio::sync::{Notify, mpsc, watch};
use tracing::{debug, error, info, trace, warn};

use nextmini_messages::lossless_session::{
    self, FecCapabilities, FecManifest, LOSSLESS_SESSION_BASE_VERSION,
    LOSSLESS_SESSION_FEC_VERSION, LosslessSessionControl,
};

use crate::node::packet::Packet;
use crate::node::processor::ProcessorHandle;
use crate::node::scheduler::token_bucket::TokenBucket;
use crate::node::session::api::InboundFrame;
use crate::node::session::control;
use crate::node::session::fec;
use crate::node::session::runtime::{CommonConfig, SenderConfig};
use crate::node::{NodeId, NodeIdExt};

pub(super) const DEFAULT_WINDOW: usize = 512;
const MANIFEST_RETRY_INTERVAL_MS: u64 = 250;
const CONTROL_POLL_TIMEOUT_MS: u64 = 20;
const ALL_FEC_LANES_BLOCKED_WAIT_MS: u64 = 250;
const TRANSFER_TIMEOUT_SECS: u64 = 300;
const FEC_REPAIR_BUDGET_DIVISOR: usize = 2;
const FEC_MAX_REPAIR_BUDGET_PER_BLOCK: usize = 64;

/// Drives a sender session: streams chunks, tracks inflight state, and reacts
/// to control frames emitted by receivers.
pub async fn run(
    cfg: SenderConfig,
    mut ctrl_rx: mpsc::Receiver<InboundFrame>,
    processors: ProcessorHandle,
) {
    let sid = cfg.common.session_id;
    let chunk_size = cfg.common.chunk_size as u64;
    let total_bytes = cfg.total_bytes;
    let total_chunks = if chunk_size == 0 {
        0
    } else {
        total_bytes.div_ceil(chunk_size)
    };

    info!(
        session_id = sid,
        total_bytes,
        total_chunks,
        receivers = cfg.receiver_ids.len(),
        fec = cfg.fec_manifest.is_some(),
        "Lossless sender started"
    );

    let chunk_bytes = cfg.common.chunk_size;
    let source_buffer = cfg.source_buffer.clone();
    let mut state = SenderState::new(cfg, total_chunks);
    let mut chunk_source = ChunkSource::new(source_buffer, chunk_bytes, total_chunks, total_bytes);
    let mut data_pacer = DataPacer::new(state.common.data_bucket.clone());
    let transfer_start = Instant::now();
    let transfer_timeout = Duration::from_secs(TRANSFER_TIMEOUT_SECS);

    loop {
        // checks for transfer timeout
        if transfer_start.elapsed() > transfer_timeout && !state.is_complete() {
            error!(
                session_id = sid,
                elapsed_secs = transfer_start.elapsed().as_secs(),
                inflight = state.inflight_len(),
                "Lossless sender: transfer timeout exceeded; forcing completion"
            );
            break;
        }
        // drains any immediately-available control frames so retire
        // decisions reflect fresh receiver state before we transmit more data
        while let Ok(frame) = ctrl_rx.try_recv() {
            state.handle_control(frame);
        }

        state.maybe_release_topology_gate();
        state.maybe_release_ready_gate();

        let mut progressed = false;

        if state.should_emit_manifest() {
            state.send_manifest(&processors).await;
            progressed = true;
        }

        // explicitly mark source as drained when chunk source finishes
        // to prevent infinite loop waiting for source_drained to be set
        if chunk_source.finished() && !state.source_drained {
            // marks this sender as drained
            state.mark_source_drained();
            progressed = true;
        }

        if state.is_fec_session() {
            if state
                .drive_fec_scheduler(&mut chunk_source, &mut data_pacer, &processors)
                .await
            {
                progressed = true;
            }
        } else if state.ready_for_data()
            && !chunk_source.finished()
            && state.inflight_len() < state.window_limit()
        {
            debug!(
                session_id = sid,
                ready_for_data = state.ready_for_data(),
                chunk_source_finished = chunk_source.finished(),
                inflight_len = state.inflight_len(),
                window = state.window_limit(),
                "Lossless sender: attempting to send next chunk."
            );
            match chunk_source.next_chunk() {
                Some(chunk) => {
                    debug!(
                        session_id = sid,
                        chunk_index = chunk.index,
                        chunk_size = chunk.data.len(),
                        "Lossless sender: sending data chunk."
                    );
                    data_pacer.wait_for(state.common.chunk_size).await;
                    state.send_data_chunk(chunk, &processors).await;
                    progressed = true;
                }
                None => {
                    state.mark_source_drained();
                    progressed = true;
                }
            }
        }

        if !progressed && state.try_emit_eot(&processors).await {
            progressed = true;
        }

        if state.is_complete() {
            if state.aborted {
                warn!(
                    session_id = sid,
                    reason = state.abort_reason.as_deref().unwrap_or("unknown"),
                    "Lossless sender aborted"
                );
            } else {
                info!(
                    session_id = sid,
                    bytes_sent = state.bytes_sent,
                    chunks_sent = state.primary_chunks,
                    "Lossless sender finished with lossless delivery guarantees"
                );
            }
            break;
        }

        if !progressed {
            if state.fec_dispatch_blocked_on_all_lanes() {
                if let Some(wakeup) = state.fec_dispatch_wakeup() {
                    let wait_result = tokio::time::timeout(
                        Duration::from_millis(ALL_FEC_LANES_BLOCKED_WAIT_MS),
                        async {
                            tokio::select! {
                                maybe_frame = ctrl_rx.recv() => Some(maybe_frame),
                                _ = wakeup.notified() => None,
                            }
                        },
                    )
                    .await;

                    if let Ok(Some(Some(frame))) = wait_result {
                        state.handle_control(frame);
                    }
                } else if let Ok(Some(frame)) = tokio::time::timeout(
                    Duration::from_millis(CONTROL_POLL_TIMEOUT_MS),
                    ctrl_rx.recv(),
                )
                .await
                {
                    state.handle_control(frame);
                }
            } else {
                // Don't block forever; let the loop re-check timers (e.g., MANIFEST resend).
                if let Ok(Some(frame)) = tokio::time::timeout(
                    Duration::from_millis(CONTROL_POLL_TIMEOUT_MS),
                    ctrl_rx.recv(),
                )
                .await
                {
                    state.handle_control(frame);
                }
                // On timeout or channel closed, just fall through and loop; this allows
                // MANIFEST re-sends every 250ms while waiting for READY.
            }
        }
    }
}

/// Tree-unassigned FEC work item produced by the scheduler.
#[derive(Debug)]
struct FecSymbolWorkItem {
    block_id: u64,
    symbol_id: u32,
    payload: Bytes,
    is_repair: bool,
}

#[derive(Debug, Clone, Copy)]
struct FecLaneSessionMeta {
    session_id: u64,
    src_ip: Ipv4Addr,
    dst_ip: Ipv4Addr,
    src_port: u16,
    dst_port: u16,
}

#[derive(Debug)]
struct FecTreeLane {
    tree_id: u16,
    tx: mpsc::Sender<FecSymbolWorkItem>,
}

#[derive(Debug)]
enum FecDispatchEnqueueResult {
    Queued {
        tree_id: u16,
    },
    AllBlocked {
        work_item: FecSymbolWorkItem,
    },
    Closed {
        tree_id: u16,
        work_item: FecSymbolWorkItem,
    },
}

#[derive(Debug)]
struct FecTreeDispatch {
    lanes: Vec<FecTreeLane>,
    next_rr_idx: usize,
    wakeup: Arc<Notify>,
    outstanding_symbols: Arc<AtomicUsize>,
    all_lanes_blocked: bool,
}

impl FecTreeDispatch {
    fn spawn(
        tree_ids: &[u16],
        lane_depth: usize,
        processors: &ProcessorHandle,
        session: FecLaneSessionMeta,
    ) -> Self {
        let wakeup = Arc::new(Notify::new());
        let outstanding_symbols = Arc::new(AtomicUsize::new(0));
        let mut lanes = Vec::with_capacity(tree_ids.len());
        let lane_depth = lane_depth.max(1);

        for tree_id in tree_ids.iter().copied() {
            let (tx, mut rx) = mpsc::channel::<FecSymbolWorkItem>(lane_depth);
            let wakeup = Arc::clone(&wakeup);
            let outstanding_symbols = Arc::clone(&outstanding_symbols);
            let processors = processors.clone();

            tokio::spawn(async move {
                while let Some(symbol) = rx.recv().await {
                    // Slot freed: wake dispatchers waiting on lane capacity.
                    wakeup.notify_waiters();

                    let frame = Bytes::from(lossless_session::encode_fec_data(
                        session.session_id,
                        symbol.block_id,
                        symbol.symbol_id,
                        tree_id,
                        &symbol.payload,
                    ));
                    let packet = Packet::build_ipv4_tcp_packet(
                        session.src_ip,
                        session.src_port,
                        session.dst_ip,
                        session.dst_port,
                        &frame,
                    );
                    processors.process_packet_blocking(packet);
                    outstanding_symbols.fetch_sub(1, Ordering::Relaxed);
                    wakeup.notify_waiters();
                }
                wakeup.notify_waiters();
            });

            lanes.push(FecTreeLane { tree_id, tx });
        }

        Self {
            lanes,
            next_rr_idx: 0,
            wakeup,
            outstanding_symbols,
            all_lanes_blocked: false,
        }
    }

    fn has_outstanding_symbols(&self) -> bool {
        self.outstanding_symbols.load(Ordering::Relaxed) > 0
    }

    fn blocked_on_all_lanes(&self) -> bool {
        self.all_lanes_blocked
    }

    fn wakeup_handle(&self) -> Arc<Notify> {
        Arc::clone(&self.wakeup)
    }

    fn try_enqueue(&mut self, work_item: FecSymbolWorkItem) -> FecDispatchEnqueueResult {
        if self.lanes.is_empty() {
            self.all_lanes_blocked = false;
            return FecDispatchEnqueueResult::Closed {
                tree_id: lossless_session::LosslessSessionFecData::DEFAULT_TREE_ID,
                work_item,
            };
        }

        let lane_count = self.lanes.len();
        let start_idx = self.next_rr_idx % lane_count;
        let mut work_item = work_item;

        for step in 0..lane_count {
            let idx = (start_idx + step) % lane_count;
            let lane = &self.lanes[idx];
            match lane.tx.try_send(work_item) {
                Ok(()) => {
                    self.next_rr_idx = (idx + 1) % lane_count;
                    self.all_lanes_blocked = false;
                    self.outstanding_symbols.fetch_add(1, Ordering::Relaxed);
                    return FecDispatchEnqueueResult::Queued {
                        tree_id: lane.tree_id,
                    };
                }
                Err(mpsc::error::TrySendError::Full(returned)) => {
                    work_item = returned;
                }
                Err(mpsc::error::TrySendError::Closed(returned)) => {
                    self.all_lanes_blocked = false;
                    return FecDispatchEnqueueResult::Closed {
                        tree_id: lane.tree_id,
                        work_item: returned,
                    };
                }
            }
        }

        self.all_lanes_blocked = true;
        FecDispatchEnqueueResult::AllBlocked { work_item }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct FecBlockStats {
    source_symbols: usize,
    repair_budget: usize,
    repairs_sent: usize,
}

#[derive(Debug, Default)]
struct FecSymbolSupply {
    pending: VecDeque<FecSymbolWorkItem>,
}

impl FecSymbolSupply {
    fn is_empty(&self) -> bool {
        self.pending.is_empty()
    }

    fn len(&self) -> usize {
        self.pending.len()
    }

    fn pop_front(&mut self) -> Option<FecSymbolWorkItem> {
        self.pending.pop_front()
    }

    fn push_front(&mut self, work_item: FecSymbolWorkItem) {
        self.pending.push_front(work_item);
    }

    fn push_symbols(
        &mut self,
        block_id: u64,
        symbols: impl IntoIterator<Item = fec::EncodedSymbol>,
        is_repair: bool,
    ) {
        for symbol in symbols {
            self.pending.push_back(FecSymbolWorkItem {
                block_id,
                symbol_id: symbol.esi,
                payload: Bytes::from(symbol.payload),
                is_repair,
            });
        }
    }
}

#[derive(Debug)]
enum FecPlanResult {
    BlockQueued {
        block_id: u64,
        source_symbols: usize,
        repair_budget: usize,
    },
    SourceDrained,
}

#[derive(Debug)]
struct FecScheduler {
    symbols_per_block: usize,
    symbol_size: usize,
    next_block_id: u64,
    active_blocks: BTreeMap<u64, FecBlockStats>,
    symbol_supply: FecSymbolSupply,
}

impl FecScheduler {
    fn new(manifest: FecManifest) -> Self {
        Self {
            symbols_per_block: usize::from(manifest.symbols_per_block.max(1)),
            symbol_size: usize::from(manifest.symbol_size.max(1)),
            next_block_id: 0,
            active_blocks: BTreeMap::new(),
            symbol_supply: FecSymbolSupply::default(),
        }
    }

    fn planned_block_count(&self) -> u64 {
        self.next_block_id
    }

    fn has_pending_symbols(&self) -> bool {
        !self.symbol_supply.is_empty()
    }

    fn pending_symbol_count(&self) -> usize {
        self.symbol_supply.len()
    }

    fn next_symbol_work_item(&mut self) -> Option<FecSymbolWorkItem> {
        self.symbol_supply.pop_front()
    }

    fn requeue_symbol_front(&mut self, work_item: FecSymbolWorkItem) {
        self.symbol_supply.push_front(work_item);
    }

    fn block_stats(&self, block_id: u64) -> Option<FecBlockStats> {
        self.active_blocks.get(&block_id).copied()
    }

    fn note_repair_symbol_sent(&mut self, block_id: u64) {
        if let Some(stats) = self.active_blocks.get_mut(&block_id) {
            stats.repairs_sent = stats.repairs_sent.saturating_add(1);
        }
    }

    fn compute_repair_budget(source_symbols: usize) -> usize {
        if source_symbols == 0 {
            return 0;
        }
        source_symbols
            .div_ceil(FEC_REPAIR_BUDGET_DIVISOR)
            .clamp(1, FEC_MAX_REPAIR_BUDGET_PER_BLOCK)
            .min(source_symbols)
    }

    fn plan_next_block(
        &mut self,
        session_id: u64,
        chunk_source: &mut ChunkSource,
    ) -> Result<FecPlanResult, String> {
        let mut chunks = Vec::with_capacity(self.symbols_per_block);

        while chunks.len() < self.symbols_per_block {
            match chunk_source.next_chunk() {
                Some(chunk) => chunks.push(chunk),
                None => break,
            }
        }

        if chunks.is_empty() {
            return Ok(FecPlanResult::SourceDrained);
        }

        let block_id = self.next_block_id;
        let source_symbols_in_block = chunks.len();
        let mut source_symbols: Vec<Vec<u8>> = Vec::with_capacity(self.symbols_per_block);

        for chunk in &chunks {
            if chunk.data.len() > self.symbol_size {
                return Err(format!(
                    "fec symbol_size {} is smaller than source chunk {} bytes",
                    self.symbol_size,
                    chunk.data.len()
                ));
            }
            let mut padded = vec![0u8; self.symbol_size];
            padded[..chunk.data.len()].copy_from_slice(&chunk.data);
            source_symbols.push(padded);
        }

        while source_symbols.len() < self.symbols_per_block {
            source_symbols.push(vec![0u8; self.symbol_size]);
        }

        let params = fec::BlockParams::new(
            source_symbols.len(),
            self.symbol_size,
            fec::block_seed(session_id, block_id),
        );
        let Some(mut encoder) = fec::Encoder::from_block(params, &source_symbols) else {
            return Err(format!(
                "failed to initialize FEC encoder for block {block_id}"
            ));
        };

        let systematic = encoder.emit_systematic();
        let repair_budget = Self::compute_repair_budget(systematic.len());
        let repairs = encoder.emit_repair(repair_budget);

        self.symbol_supply.push_symbols(block_id, systematic, false);
        self.symbol_supply.push_symbols(block_id, repairs, true);
        self.active_blocks.insert(
            block_id,
            FecBlockStats {
                source_symbols: source_symbols_in_block,
                repair_budget,
                repairs_sent: 0,
            },
        );
        self.next_block_id = self.next_block_id.saturating_add(1);

        Ok(FecPlanResult::BlockQueued {
            block_id,
            source_symbols: source_symbols_in_block,
            repair_budget,
        })
    }
}

/// Encapsulates all mutable sender-side state (window, inflight accounting,
/// pacing, manifest timing, etc.). Keeping the logic centralized makes the event
/// loop above easier to read and test.

struct SenderState {
    session_id: u64,
    common: CommonConfig,
    fec_manifest: Option<FecManifest>,
    fec_tree_ids: Vec<u16>,
    fec_tree_lane_depth: usize,
    fec_dispatch_burst: usize,
    receiver_count: usize,
    window: usize,
    total_chunks: u64,
    total_bytes: u64,
    receiver_progress: BTreeMap<usize, u64>,
    receiver_completed_fec_blocks: BTreeMap<usize, BTreeSet<u64>>,
    fec_capabilities: BTreeMap<usize, FecCapabilities>,
    fec_incompatible_peers: HashSet<usize>,
    retired_up_to: u64,
    ready_nodes: HashSet<usize>,
    topology_gate_open: bool,
    ready_gate_open: bool,
    ready_deadline: Option<Instant>,
    topology_ready_rx: Option<watch::Receiver<bool>>,
    ready_grace: Duration,
    manifest_sent: bool,
    manifest_last_sent: Instant,
    manifest_interval: Duration,
    source_drained: bool,
    eot_sent: bool,
    aborted: bool,
    abort_reason: Option<String>,
    primary_chunks: u64,
    fec_scheduler: Option<FecScheduler>,
    fec_dispatch: Option<FecTreeDispatch>,
    bytes_sent: u64,
    src_ip: Ipv4Addr,
    dst_ip: Ipv4Addr,
    src_port: u16,
    dst_port: u16,
    // Throughput tracking
    throughput_start: Instant,
    throughput_last_report: Instant,
    bytes_since_last_report: u64,
}

impl SenderState {
    /// Builds a fresh state tracker for a sender session, wiring gate watchers
    /// and initializing per-receiver progress counters.
    fn new(mut cfg: SenderConfig, total_chunks: u64) -> Self {
        let common = cfg.common.clone();
        let fec_manifest = cfg.fec_manifest;
        let fec_tree_ids = cfg.fec_tree_ids.clone();
        let fec_tree_lane_depth = cfg.fec_tree_lane_depth;
        let fec_dispatch_burst = cfg.fec_dispatch_burst;
        let receiver_count = cfg.receiver_ids.len();

        let ready_gate_open = receiver_count == 0;
        let ready_grace = Duration::from_millis(cfg.ready_grace_ms.max(1));

        let topology_ready_rx = cfg.topology_ready.take();
        let topology_gate_open = topology_ready_rx
            .as_ref()
            .map(|rx| *rx.borrow())
            .unwrap_or(true);

        let ready_deadline = None;

        let src_ip = (common.local_node_id as NodeId)
            .ip_addr(common.user_space_base_addr, common.local_netmask);
        let dst_ip = common.dest_ip;
        let window = compute_window(&cfg);

        let mut receiver_progress = BTreeMap::new();
        let mut receiver_completed_fec_blocks = BTreeMap::new();
        for node_id in &cfg.receiver_ids {
            receiver_progress.insert(*node_id, 0);
            receiver_completed_fec_blocks.insert(*node_id, BTreeSet::new());
        }

        let mut state = Self {
            session_id: common.session_id,
            common,
            fec_manifest,
            fec_tree_ids,
            fec_tree_lane_depth,
            fec_dispatch_burst,
            receiver_count,
            window,
            total_chunks,
            total_bytes: cfg.total_bytes,
            receiver_progress,
            receiver_completed_fec_blocks,
            fec_capabilities: BTreeMap::new(),
            fec_incompatible_peers: HashSet::new(),
            retired_up_to: 0,
            ready_nodes: HashSet::new(),
            topology_gate_open,
            ready_gate_open,
            ready_deadline,
            topology_ready_rx,
            ready_grace,
            manifest_sent: false,
            manifest_last_sent: Instant::now(),
            manifest_interval: Duration::from_millis(MANIFEST_RETRY_INTERVAL_MS),
            source_drained: total_chunks == 0,
            eot_sent: false,
            aborted: false,
            abort_reason: None,
            primary_chunks: 0,
            fec_scheduler: fec_manifest.map(FecScheduler::new),
            fec_dispatch: None,
            bytes_sent: 0,
            src_ip,
            dst_ip,
            src_port: cfg.common.src_port,
            dst_port: cfg.common.dst_port,
            throughput_start: Instant::now(),
            throughput_last_report: Instant::now(),
            bytes_since_last_report: 0,
        };
        state.update_retired_up_to();
        state
    }

    /// Emit a MANIFEST describing the file transfer so receivers can prime their state.
    async fn send_manifest(&mut self, processors: &ProcessorHandle) {
        let manifest = if let Some(fec) = self.fec_manifest {
            LosslessSessionControl::FecManifest {
                chunk_size: self.common.chunk_size as u32,
                total_bytes: self.total_bytes,
                fec,
            }
        } else {
            LosslessSessionControl::Manifest {
                chunk_size: self.common.chunk_size as u32,
                total_bytes: self.total_bytes,
            }
        };
        self.send_control(&manifest, processors).await;
        self.manifest_last_sent = Instant::now();
        if !self.manifest_sent && self.receiver_count > 0 && self.ready_deadline.is_none() {
            self.ready_deadline = Some(Instant::now() + self.ready_grace);
        }
        self.manifest_sent = true;
        info!(
            session_id = self.session_id,
            fec = self.fec_manifest.is_some(),
            src = %self.src_ip,
            dst = %self.dst_ip,
            dst_port = self.dst_port,
            total_bytes = self.total_bytes,
            chunk_size = self.common.chunk_size,
            receivers = self.receiver_count,
            "Lossless sender: MANIFEST sent"
        );
    }

    /// Determines whether the sender is allowed to transmit data frames.
    fn ready_for_data(&self) -> bool {
        self.topology_gate_open
            && self.ready_gate_open
            && !self.aborted
            && self.fec_preflight_satisfied()
            && (!self.source_drained || self.has_pending_fec_symbols())
    }

    fn has_pending_fec_symbols(&self) -> bool {
        let scheduler_pending = self.has_scheduler_pending_fec_symbols();
        let lane_pending = self.has_pending_fec_lane_symbols();
        scheduler_pending || lane_pending
    }

    fn has_scheduler_pending_fec_symbols(&self) -> bool {
        self.fec_scheduler
            .as_ref()
            .is_some_and(FecScheduler::has_pending_symbols)
    }

    fn has_pending_fec_lane_symbols(&self) -> bool {
        self.fec_dispatch
            .as_ref()
            .is_some_and(FecTreeDispatch::has_outstanding_symbols)
    }

    fn fec_blocks_planned(&self) -> u64 {
        self.fec_scheduler
            .as_ref()
            .map(FecScheduler::planned_block_count)
            .unwrap_or_default()
    }

    #[inline]
    fn is_fec_session(&self) -> bool {
        self.fec_manifest.is_some()
    }

    #[inline]
    fn control_protocol_version(&self) -> u8 {
        if self.is_fec_session() {
            LOSSLESS_SESSION_FEC_VERSION
        } else {
            LOSSLESS_SESSION_BASE_VERSION
        }
    }

    fn has_all_fec_capabilities(&self) -> bool {
        self.receiver_progress
            .keys()
            .all(|peer| self.fec_capabilities.contains_key(peer))
    }

    fn fec_preflight_satisfied(&self) -> bool {
        if !self.is_fec_session() {
            return true;
        }
        self.fec_incompatible_peers.is_empty() && self.has_all_fec_capabilities()
    }

    /// Ensures the window never collapses to zero (which would deadlock the loop).
    fn window_limit(&self) -> usize {
        if let Some(manifest) = self.fec_manifest {
            let symbols_per_block = usize::from(manifest.symbols_per_block.max(1));
            return self.window.div_ceil(symbols_per_block).max(1);
        }
        self.window.max(1)
    }

    /// Returns the number of outstanding transfer units waiting for receiver retirement.
    /// Non-FEC sessions track chunks; FEC sessions track blocks.
    fn inflight_len(&self) -> usize {
        self.outstanding_units() as usize
    }

    /// Returns the number of chunks or blocks currently outside of the retired window.
    fn outstanding_units(&self) -> u64 {
        if self.is_fec_session() {
            self.fec_blocks_planned().saturating_sub(self.retired_up_to)
        } else {
            self.primary_chunks.saturating_sub(self.retired_up_to)
        }
    }

    fn total_fec_blocks(&self) -> u64 {
        let Some(manifest) = self.fec_manifest else {
            return 0;
        };
        let symbols_per_block = u64::from(manifest.symbols_per_block.max(1));
        self.total_chunks.div_ceil(symbols_per_block)
    }

    fn all_required_fec_blocks_planned(&self) -> bool {
        if !self.is_fec_session() || !self.source_drained {
            return true;
        }
        self.fec_blocks_planned() >= self.total_fec_blocks()
    }

    fn required_work_tracking_consistent(&self) -> bool {
        self.outstanding_units() == self.inflight_len() as u64
    }

    fn completion_guards_satisfied(&self) -> bool {
        let scheduler_drained = !self.has_scheduler_pending_fec_symbols();
        let lanes_drained = !self.has_pending_fec_lane_symbols();
        let required_work_done = self.outstanding_units() == 0;

        scheduler_drained
            && lanes_drained
            && required_work_done
            && self.all_required_fec_blocks_planned()
            && self.required_work_tracking_consistent()
    }

    fn ready_to_emit_eot(&self) -> bool {
        !self.eot_sent && self.source_drained && self.completion_guards_satisfied()
    }

    /// Decide whether we should re-send the MANIFEST while the ready gate stays closed.
    fn should_resend_manifest(&self) -> bool {
        self.manifest_sent
            && self.topology_gate_open
            && !self.ready_gate_open
            && !self.aborted
            && self.manifest_last_sent.elapsed() >= self.manifest_interval
    }

    /// Determines if the sender should emit a MANIFEST based on topology/gate state.
    fn should_emit_manifest(&self) -> bool {
        if !self.topology_gate_open {
            return false;
        }
        if !self.manifest_sent {
            return true;
        }
        self.should_resend_manifest()
    }

    /// Marks the source buffer as fully drained (preventing redundant reads).
    fn mark_source_drained(&mut self) {
        self.source_drained = true;
    }

    // reports throughput every 1 second
    fn report_throughput(&mut self) {
        let now = Instant::now();
        let elapsed = now
            .duration_since(self.throughput_last_report)
            .as_secs_f64();

        if elapsed >= 1.0 && self.bytes_since_last_report > 0 {
            let throughput_gbps =
                (self.bytes_since_last_report as f64 * 8.0) / (elapsed * 1_000_000_000.0);
            let total_elapsed = now.duration_since(self.throughput_start).as_secs_f64();

            info!(
                session_id = self.session_id,
                throughput_gbps = format!("{:.3}", throughput_gbps),
                bytes = self.bytes_since_last_report,
                elapsed_s = format!("{:.3}", elapsed),
                total_sent = self.bytes_sent,
                total_elapsed_s = format!("{:.3}", total_elapsed),
                "Lossless sender: throughput"
            );

            self.bytes_since_last_report = 0;
            self.throughput_last_report = now;
        }
    }

    fn ensure_fec_dispatch_initialized(&mut self, processors: &ProcessorHandle) -> bool {
        if !self.is_fec_session() || self.fec_dispatch.is_some() {
            return true;
        }
        if self.fec_tree_ids.is_empty() {
            self.abort_fec_preflight(
                "multi-tree FEC dispatch requires a non-empty configured tree-id set",
            );
            return false;
        }

        let session = FecLaneSessionMeta {
            session_id: self.session_id,
            src_ip: self.src_ip,
            dst_ip: self.dst_ip,
            src_port: self.src_port,
            dst_port: self.dst_port,
        };
        self.fec_dispatch = Some(FecTreeDispatch::spawn(
            &self.fec_tree_ids,
            self.fec_tree_lane_depth,
            processors,
            session,
        ));

        info!(
            session_id = self.session_id,
            tree_ids = ?self.fec_tree_ids,
            lane_depth = self.fec_tree_lane_depth,
            "Lossless sender: started collaborative per-tree FEC lanes"
        );
        true
    }

    fn fec_dispatch_blocked_on_all_lanes(&self) -> bool {
        self.fec_dispatch
            .as_ref()
            .is_some_and(FecTreeDispatch::blocked_on_all_lanes)
    }

    fn fec_dispatch_wakeup(&self) -> Option<Arc<Notify>> {
        self.fec_dispatch
            .as_ref()
            .map(FecTreeDispatch::wakeup_handle)
    }

    async fn drive_fec_scheduler(
        &mut self,
        chunk_source: &mut ChunkSource,
        data_pacer: &mut DataPacer,
        processors: &ProcessorHandle,
    ) -> bool {
        if !self.ready_for_data() {
            return false;
        }
        if !self.ensure_fec_dispatch_initialized(processors) {
            return false;
        }

        let mut progressed = false;
        if self.maybe_queue_next_fec_block(chunk_source) {
            progressed = true;
        }
        for _ in 0..self.fec_dispatch_burst.max(1) {
            if self.try_send_next_fec_symbol(data_pacer).await {
                progressed = true;
            } else {
                break;
            }
        }
        progressed
    }

    /// Materialize the next FEC block (systematic + bounded repairs) into the scheduler queue.
    fn maybe_queue_next_fec_block(&mut self, chunk_source: &mut ChunkSource) -> bool {
        if self.fec_scheduler.is_none() {
            return false;
        }
        if self.has_scheduler_pending_fec_symbols()
            || self.source_drained
            || self.inflight_len() >= self.window_limit()
        {
            return false;
        }

        let plan_result = {
            let scheduler = self
                .fec_scheduler
                .as_mut()
                .expect("fec scheduler must exist in FEC sessions");
            scheduler.plan_next_block(self.session_id, chunk_source)
        };

        match plan_result {
            Ok(FecPlanResult::SourceDrained) => {
                self.mark_source_drained();
                true
            }
            Ok(FecPlanResult::BlockQueued {
                block_id,
                source_symbols,
                repair_budget,
            }) => {
                let symbols_queued = self
                    .fec_scheduler
                    .as_ref()
                    .map(FecScheduler::pending_symbol_count)
                    .unwrap_or_default();
                self.update_retired_up_to();
                debug!(
                    session_id = self.session_id,
                    block_id,
                    source_symbols,
                    repairs = repair_budget,
                    symbols_queued,
                    "Lossless sender: queued FEC block symbols"
                );
                true
            }
            Err(reason) => {
                self.abort_fec_preflight(reason);
                true
            }
        }
    }

    async fn try_send_next_fec_symbol(&mut self, data_pacer: &mut DataPacer) -> bool {
        let Some(symbol) = self
            .fec_scheduler
            .as_mut()
            .and_then(FecScheduler::next_symbol_work_item)
        else {
            if let Some(dispatch) = self.fec_dispatch.as_mut() {
                dispatch.all_lanes_blocked = false;
            }
            return false;
        };

        data_pacer.wait_for(symbol.payload.len()).await;
        let block_id = symbol.block_id;
        let symbol_id = symbol.symbol_id;
        let payload_len = symbol.payload.len();
        let is_repair = symbol.is_repair;

        let Some(dispatch) = self.fec_dispatch.as_mut() else {
            self.abort_fec_preflight("fec dispatch lanes are unavailable in FEC session");
            return true;
        };

        match dispatch.try_enqueue(symbol) {
            FecDispatchEnqueueResult::Queued { tree_id } => {
                let payload_len_u64 = payload_len as u64;
                self.bytes_sent += payload_len_u64;
                self.bytes_since_last_report += payload_len_u64;
                if !is_repair {
                    self.primary_chunks += 1;
                } else if let Some(scheduler) = self.fec_scheduler.as_mut() {
                    scheduler.note_repair_symbol_sent(block_id);
                }
                self.report_throughput();

                debug!(
                    session_id = self.session_id,
                    block_id,
                    symbol_id,
                    tree_id,
                    is_repair,
                    payload_len,
                    "Lossless sender: dispatched FEC symbol to tree lane"
                );
                true
            }
            FecDispatchEnqueueResult::AllBlocked { work_item } => {
                if let Some(scheduler) = self.fec_scheduler.as_mut() {
                    scheduler.requeue_symbol_front(work_item);
                }
                trace!(
                    session_id = self.session_id,
                    block_id,
                    symbol_id,
                    "Lossless sender: all FEC tree lanes are backpressured; waiting for wakeup"
                );
                false
            }
            FecDispatchEnqueueResult::Closed { tree_id, work_item } => {
                if let Some(scheduler) = self.fec_scheduler.as_mut() {
                    scheduler.requeue_symbol_front(work_item);
                }
                self.abort_fec_preflight(format!(
                    "FEC dispatch lane for tree {tree_id} closed unexpectedly"
                ));
                true
            }
        }
    }

    /// Encode and hand off a non-FEC chunk to the processor, updating accounting.
    async fn send_data_chunk(&mut self, chunk: ChunkPayload, processors: &ProcessorHandle) {
        let frame = Bytes::from(lossless_session::encode_data(
            self.session_id,
            chunk.index,
            &chunk.data,
        ));

        let chunk_bytes = chunk.data.len() as u64;
        self.bytes_sent += chunk_bytes;
        self.bytes_since_last_report += chunk_bytes;
        self.primary_chunks += 1;
        self.update_retired_up_to();
        self.report_throughput();
        debug!(
            session_id = self.session_id,
            chunk_index = chunk.index,
            chunk_data_len = chunk.data.len(),
            src_ip = %self.src_ip,
            dst_ip = %self.dst_ip,
            "Lossless sender: encoded DATA chunk, sending frame to processor"
        );
        self.send_frame(&frame, processors).await;
    }

    /// Advance the retired watermark based on the slowest receiver.
    fn update_retired_up_to(&mut self) {
        if self.receiver_count == 0 {
            self.retired_up_to = if self.is_fec_session() {
                self.fec_blocks_planned()
            } else {
                self.primary_chunks
            };
            return;
        }
        if self.receiver_progress.is_empty() {
            return;
        }
        let min_progress = self
            .receiver_progress
            .values()
            .copied()
            .min()
            .unwrap_or(self.retired_up_to);
        self.retired_up_to = if self.is_fec_session() {
            min_progress
                .min(self.total_fec_blocks())
                .min(self.fec_blocks_planned())
        } else {
            min_progress.min(self.total_chunks)
        };
    }

    fn update_receiver_fec_status(
        &mut self,
        from_node: usize,
        status: &lossless_session::FecStatus,
    ) -> Option<u64> {
        if status.deficit_symbols != 0 {
            return None;
        }
        if !self.receiver_progress.contains_key(&from_node) {
            return None;
        }

        let total_blocks = self.total_fec_blocks();
        if status.block_id >= total_blocks {
            trace!(
                session_id = self.session_id,
                from_node,
                block_id = status.block_id,
                total_blocks,
                "Lossless sender: ignoring out-of-range FEC status block"
            );
            return None;
        }

        let mut contiguous_completed = self
            .receiver_progress
            .get(&from_node)
            .copied()
            .unwrap_or_default();
        if status.block_id < contiguous_completed {
            return None;
        }

        let completed_blocks = self
            .receiver_completed_fec_blocks
            .entry(from_node)
            .or_default();
        if !completed_blocks.insert(status.block_id) {
            return None;
        }

        let mut advanced = false;
        while completed_blocks.remove(&contiguous_completed) {
            contiguous_completed = contiguous_completed.saturating_add(1);
            advanced = true;
        }
        if !advanced {
            return None;
        }

        if let Some(progress) = self.receiver_progress.get_mut(&from_node) {
            *progress = contiguous_completed;
            return Some(*progress);
        }
        None
    }

    /// Emit an End-of-Transfer once all units have been acknowledged.
    async fn try_emit_eot(&mut self, processors: &ProcessorHandle) -> bool {
        if !self.ready_to_emit_eot() {
            if !self.eot_sent && self.source_drained {
                trace!(
                    session_id = self.session_id,
                    scheduler_pending = self.has_scheduler_pending_fec_symbols(),
                    lane_pending = self.has_pending_fec_lane_symbols(),
                    inflight_count = self.outstanding_units(),
                    required_work_tracking_consistent = self.required_work_tracking_consistent(),
                    all_required_fec_blocks_planned = self.all_required_fec_blocks_planned(),
                    "Lossless sender: cannot send EOT yet"
                );
            }
            return false;
        }
        let last_index = self.eot_last_index();
        let eot = LosslessSessionControl::Eot { last_index };
        self.send_control(&eot, processors).await;
        self.eot_sent = true;

        info!(
            session_id = self.session_id,
            last_index, "Lossless sender: EOT sent"
        );
        true
    }

    fn eot_last_index(&self) -> u64 {
        // EOT remains chunk-index based for both legacy and FEC sessions.
        // Receivers gate completion on contiguous chunk reconstruction.
        self.total_chunks
    }

    /// Returns true when the sender drained the source and all acknowledgements were processed.
    fn is_complete(&self) -> bool {
        self.aborted || (self.source_drained && self.eot_sent && self.completion_guards_satisfied())
    }

    fn abort_fec_preflight(&mut self, reason: impl Into<String>) {
        if self.aborted {
            return;
        }
        let reason = reason.into();
        self.aborted = true;
        self.abort_reason = Some(reason.clone());
        self.ready_gate_open = false;
        self.source_drained = true;
        self.eot_sent = true;
        warn!(
            session_id = self.session_id,
            reason = reason,
            "Lossless sender: aborting session before FEC data emission"
        );
    }

    /// Handles READY/ACK/EOT control frames coming from receivers.
    fn handle_control(&mut self, frame: InboundFrame) {
        let InboundFrame { bytes, peer_id, .. } = frame;
        let Some((_, control)) = lossless_session::decode_control(&bytes) else {
            warn!(
                session_id = self.session_id,
                "Lossless sender: failed to decode control frame."
            );
            return;
        };
        match &control {
            LosslessSessionControl::Ready { node_id } => {
                self.ready_nodes.insert(*node_id as usize);
                info!(
                    session_id = self.session_id,
                    node_id = *node_id,
                    "Lossless sender: receiver node {} ready.",
                    *node_id
                );
            }
            LosslessSessionControl::FecCapabilities {
                node_id,
                capabilities,
            } => {
                if !self.is_fec_session() {
                    trace!(
                        session_id = self.session_id,
                        node_id = *node_id,
                        "Lossless sender: ignoring FEC capabilities in non-FEC session"
                    );
                    return;
                }
                let from_node = peer_id.unwrap_or(*node_id as usize);
                if !self.receiver_progress.contains_key(&from_node) {
                    warn!(
                        session_id = self.session_id,
                        from_node, "Lossless sender: ignoring capabilities from unexpected node"
                    );
                    return;
                }
                self.fec_capabilities.insert(from_node, *capabilities);

                let Some(manifest) = self.fec_manifest else {
                    return;
                };
                match control::ensure_fec_compatible(&manifest, capabilities) {
                    Ok(()) => {
                        self.fec_incompatible_peers.remove(&from_node);
                        debug!(
                            session_id = self.session_id,
                            from_node,
                            supported_schemes = capabilities.supported_schemes,
                            "Lossless sender: peer FEC capabilities accepted"
                        );
                    }
                    Err(err) => {
                        self.fec_incompatible_peers.insert(from_node);
                        self.abort_fec_preflight(format!(
                            "peer {from_node} incompatible with requested FEC manifest: {err:?}"
                        ));
                    }
                }
            }
            LosslessSessionControl::Manifest { .. }
            | LosslessSessionControl::FecManifest { .. }
            | LosslessSessionControl::Eot { .. } => {
                // ignores if the sender-originated control frames somehow looped back
            }
            LosslessSessionControl::Ack { .. } => {
                if self.is_fec_session() {
                    trace!(
                        session_id = self.session_id,
                        ?control,
                        "Lossless sender: ignoring cumulative ACK in FEC mode"
                    );
                    return;
                }
                let Some(from_node) = peer_id else {
                    warn!(
                        session_id = self.session_id,
                        ?control,
                        "Lossless sender: dropping control without peer id"
                    );
                    return;
                };
                if !self.receiver_progress.contains_key(&from_node) {
                    warn!(
                        session_id = self.session_id,
                        from_node, "Lossless sender: ignoring ACK from unexpected node"
                    );
                    return;
                }
                let updated = control::update_receiver_progress(
                    from_node,
                    &control,
                    &mut self.receiver_progress,
                );
                if let Some(new_value) = updated {
                    self.update_retired_up_to();
                    debug!(
                        session_id = self.session_id,
                        from_node = from_node,
                        up_to = new_value,
                        retired_up_to = self.retired_up_to,
                        inflight_count = self.outstanding_units(),
                        "Lossless sender: cumulative ACK processed"
                    );
                } else {
                    trace!(
                        session_id = self.session_id,
                        from_node = from_node,
                        "Lossless sender: ACK made no progress"
                    );
                }
            }
            LosslessSessionControl::FecStatus { status } => {
                if !self.is_fec_session() {
                    return;
                }
                let Some(from_node) = peer_id else {
                    warn!(
                        session_id = self.session_id,
                        "Lossless sender: dropping FEC status without peer id"
                    );
                    return;
                };
                if !self.receiver_progress.contains_key(&from_node) {
                    return;
                }
                if self.update_receiver_fec_status(from_node, status).is_some() {
                    self.update_retired_up_to();
                }
                let (planned_source, planned_repairs, remaining_budget) = self
                    .fec_scheduler
                    .as_ref()
                    .and_then(|scheduler| scheduler.block_stats(status.block_id))
                    .map(|stats| {
                        (
                            stats.source_symbols,
                            stats.repairs_sent,
                            stats.repair_budget.saturating_sub(stats.repairs_sent),
                        )
                    })
                    .unwrap_or((0, 0, 0));
                trace!(
                    session_id = self.session_id,
                    block_id = status.block_id,
                    deficit_symbols = status.deficit_symbols,
                    source_symbols = planned_source,
                    repairs_sent = planned_repairs,
                    repair_budget_remaining = remaining_budget,
                    "Lossless sender: receiver FEC status update"
                );
            }
        }
    }

    /// Serialize the already-encoded payload into a packet and enqueue it.
    async fn send_frame(&self, frame: &Bytes, processors: &ProcessorHandle) {
        let packet = Packet::build_ipv4_tcp_packet(
            self.src_ip,
            self.src_port,
            self.dst_ip,
            self.dst_port,
            frame,
        );

        processors.process_packet(packet).await;
    }

    /// Convenience helper for building and sending control packets.
    async fn send_control(&self, control: &LosslessSessionControl, processors: &ProcessorHandle) {
        // Use stack-allocated buffer to avoid heap allocation for small control frames
        let mut buf = [0u8; lossless_session::MAX_CONTROL_FRAME_SIZE];
        let frame = lossless_session::encode_control_into_with_version(
            &mut buf,
            self.session_id,
            self.control_protocol_version(),
            control,
        );

        let packet = Packet::build_ipv4_tcp_packet(
            self.src_ip,
            self.src_port,
            self.dst_ip,
            self.dst_port,
            frame,
        );

        processors.process_packet(packet).await;
    }

    /// Opens the topology gate once the watch channel signals readiness.
    fn maybe_release_topology_gate(&mut self) {
        if self.topology_gate_open {
            return;
        }

        let Some(rx) = self.topology_ready_rx.as_mut() else {
            self.topology_gate_open = true;
            return;
        };

        if *rx.borrow() {
            self.topology_gate_open = true;

            info!(
                session_id = self.session_id,
                "Lossless sender: topology-ready signal received."
            );
        }
    }

    /// Once every receiver signals READY (or we time out), unblock data transfer.
    fn maybe_release_ready_gate(&mut self) {
        if self.ready_gate_open || !self.manifest_sent || self.aborted {
            return;
        }

        if self.is_fec_session() {
            if let Some(manifest) = self.fec_manifest
                && manifest.scheme_kind().is_none()
            {
                self.abort_fec_preflight(format!(
                    "unknown fec scheme {} requested by sender",
                    manifest.scheme
                ));
                return;
            }
            let all_ready =
                self.receiver_count == 0 || self.ready_nodes.len() == self.receiver_count;
            let all_capabilities = self.receiver_count == 0 || self.has_all_fec_capabilities();
            let all_compatible = if let Some(manifest) = self.fec_manifest {
                let required_receivers: Vec<usize> =
                    self.receiver_progress.keys().copied().collect();
                !control::should_abort_fec_preflight(
                    &required_receivers,
                    &manifest,
                    &self.fec_capabilities,
                ) && self.fec_incompatible_peers.is_empty()
            } else {
                true
            };

            if all_ready && all_capabilities && all_compatible {
                self.ready_gate_open = true;
                self.ready_deadline = None;
                info!(
                    session_id = self.session_id,
                    "Lossless sender: all receivers accepted FEC preflight."
                );
                return;
            }

            if let Some(deadline) = self.ready_deadline
                && Instant::now() >= deadline
            {
                if !all_capabilities {
                    self.abort_fec_preflight(
                        "missing one or more FEC capability responses from required receivers",
                    );
                } else if !all_compatible {
                    self.abort_fec_preflight("at least one receiver rejected requested FEC mode");
                } else if !all_ready {
                    self.abort_fec_preflight("missing READY from one or more receivers");
                }
            }
            return;
        }

        if self.receiver_count > 0 && self.ready_nodes.len() == self.receiver_count {
            self.ready_gate_open = true;
            self.ready_deadline = None;

            info!(
                session_id = self.session_id,
                "Lossless sender: all receivers are ready."
            );

            return;
        }

        if let Some(deadline) = self.ready_deadline
            && Instant::now() >= deadline
        {
            self.ready_gate_open = true;
            self.ready_deadline = None;
            warn!(
                session_id = self.session_id,
                ready = self.ready_nodes.len(),
                total = self.receiver_count,
                "Lossless sender: proceeding without all receivers ready"
            );
        }
    }
}

/// Compute a sliding window size based on the default limit and, if present,
/// the token-bucket shaper so we never admit more inflight bytes than the
/// data_pacer can service.
fn compute_window(cfg: &SenderConfig) -> usize {
    let mut window = DEFAULT_WINDOW;

    if let Some(bucket) = &cfg.common.data_bucket
        && cfg.common.chunk_size > 0
    {
        let per_chunk = cfg.common.chunk_size;
        let bucket_chunks = (bucket.bucket_size / per_chunk).max(1);
        window = window.min(bucket_chunks);
    }

    info!("The sliding window size is {window} on the source.");

    window
}

/// Materialized chunk that is ready to be encoded into a lossless session frame.
struct ChunkPayload {
    index: u64,
    data: Bytes,
}

enum ChunkSourceKind {
    // slices data directly from an in-memory buffer, zero-copy
    Borrowed { bytes: Bytes, offset: usize },
    // reuses a chunk template for synthetic payloads
    Template { template: Bytes },
}

impl ChunkSourceKind {
    fn borrowed(bytes: Bytes) -> Self {
        Self::Borrowed { bytes, offset: 0 }
    }

    fn template(bytes: Bytes, chunk_size: usize) -> Self {
        // Pre-build a template that's at least chunk_size bytes to avoid
        // per-chunk allocations when the provided bytes are smaller.
        let template = if bytes.is_empty() && chunk_size > 0 {
            Bytes::from(vec![0u8; chunk_size])
        } else if chunk_size > bytes.len() {
            // Build a template large enough for any chunk by repeating the pattern
            let mut buf = BytesMut::with_capacity(chunk_size);
            while buf.len() < chunk_size {
                let remaining = chunk_size - buf.len();
                let take = remaining.min(bytes.len());
                buf.extend_from_slice(&bytes[..take]);
            }
            buf.freeze()
        } else {
            bytes
        };
        Self::Template { template }
    }
}

/// Reads chunk payloads from memory or a template and hands them to the sender
/// in strict index order.
struct ChunkSource {
    chunk_size: usize,
    total_chunks: u64,
    total_bytes: u64,
    next_index: u64,
    kind: ChunkSourceKind,
}

impl ChunkSource {
    /// Construct a new chunk source that will walk through the shared buffer.
    fn new(bytes: Bytes, chunk_size: usize, total_chunks: u64, total_bytes: u64) -> Self {
        let matches_len = usize::try_from(total_bytes)
            .map(|len| len == bytes.len())
            .unwrap_or(false);

        let kind = if matches_len {
            ChunkSourceKind::borrowed(bytes)
        } else {
            // Template mode: pre-build the template to avoid per-chunk allocations
            ChunkSourceKind::template(bytes, chunk_size.max(1))
        };

        Self {
            chunk_size,
            total_chunks,
            total_bytes,
            next_index: 1,
            kind,
        }
    }

    /// Returns true when every chunk has either been produced or the transfer was zero-length.
    fn finished(&self) -> bool {
        self.total_chunks == 0 || self.next_index > self.total_chunks
    }

    /// Returns the next chunk, advancing the internal cursor. For borrowed
    /// buffers we slice without copying; template mode reuses the same chunk.
    fn next_chunk(&mut self) -> Option<ChunkPayload> {
        if self.finished() {
            return None;
        }

        if self.chunk_size == 0 {
            self.next_index = self.total_chunks + 1;
            return None;
        }

        let idx = self.next_index;
        self.next_index += 1;

        let chunk_len = if idx == self.total_chunks {
            let rem = (self.total_bytes % self.chunk_size as u64) as usize;
            if rem == 0 { self.chunk_size } else { rem }
        } else {
            self.chunk_size
        };

        let data = match &mut self.kind {
            ChunkSourceKind::Borrowed { bytes, offset } => {
                let start = *offset;
                let end = start.saturating_add(chunk_len);
                if end > bytes.len() {
                    return None;
                }
                *offset = end;
                bytes.slice(start..end)
            }
            ChunkSourceKind::Template { template } => {
                // Template is pre-built to be at least chunk_size, so just slice.
                // The only time chunk_len differs is the last chunk (remainder).
                if chunk_len <= template.len() {
                    template.slice(0..chunk_len)
                } else {
                    // This should never happen with proper template pre-building,
                    // but handle it gracefully by allocating zeroes.
                    Bytes::from(vec![0u8; chunk_len])
                }
            }
        };

        Some(ChunkPayload { index: idx, data })
    }
}

/// Simple token-bucket pacer used to honor optional bandwidth caps.
///
/// This is now just a thin wrapper around the shared scheduler TokenBucket
/// implementation so that we don't duplicate token-bucket logic here.
struct DataPacer {
    bucket: Option<TokenBucket>,
}

impl DataPacer {
    /// Builds a data_pacer backed by the runtime token-bucket implementation.
    fn new(spec: Option<nextmini_messages::TokenBucketSpec>) -> Self {
        let bucket = spec.map(TokenBucket::new);
        Self { bucket }
    }

    /// Await scheduling tokens before sending `bytes` worth of payload.
    async fn wait_for(&mut self, bytes: usize) {
        if let Some(bucket) = self.bucket.as_mut() {
            bucket.wait_for_bytes(bytes).await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use nextmini_messages::{TokenBucketSpec, lossless_session};
    use std::net::Ipv4Addr;

    #[test]
    fn data_roundtrip_header_and_meta() {
        let sid = 99;
        let idx = 7;
        let plen = 4096usize;
        let payload = vec![0xAAu8; plen];
        let buf = lossless_session::encode_data(sid, idx, &payload);
        let (hdr, data, body) = lossless_session::decode_data(&buf).expect("decode data");

        assert_eq!(hdr.session_id, sid);
        assert_eq!(data.index, idx);
        assert_eq!(data.payload_len as usize, plen);
        assert_eq!(body.len(), plen);
    }

    /// Validates ChunkSource properly reports when finished.
    ///
    /// This tests the fix for the infinite loop bug where the sender could get stuck
    /// waiting for source_drained when chunk_source.finished() returned true but
    /// the state wasn't updated.
    #[test]
    fn chunk_source_finished_detection() {
        // Zero chunks should be immediately finished
        let source = ChunkSource::new(Bytes::new(), 1024, 0, 0);
        assert!(
            source.finished(),
            "ChunkSource with 0 chunks should be finished immediately"
        );

        //Source with chunks should not be finished initially
        let mut source =
            ChunkSource::new(Bytes::from(vec![0u8; 1024 * 5]), 1024, 5, (1024 * 5) as u64);
        assert!(
            !source.finished(),
            "ChunkSource with 5 chunks should not be finished initially"
        );

        // After consuming all chunks, should be finished
        for _ in 0..5 {
            let result = source.next_chunk();
            assert!(result.is_some(), "Should successfully get chunk");
        }
        assert!(
            source.finished(),
            "ChunkSource should be finished after all chunks consumed"
        );

        // Requesting more chunks after finished returns None
        let result = source.next_chunk();
        assert!(result.is_none(), "Should return None when finished");
    }

    #[test]
    fn chunk_source_template_mode_reuses_buffer() {
        let chunk_size = 1024;
        let total_bytes = (chunk_size as u64 * 3) + 512;
        let total_chunks = total_bytes.div_ceil(chunk_size as u64);
        let template = Bytes::from(vec![0xBBu8; chunk_size]);
        let mut source = ChunkSource::new(template, chunk_size, total_chunks, total_bytes);

        for _ in 0..(total_chunks - 1) {
            let chunk = source.next_chunk().expect("chunk must be available");
            assert_eq!(
                chunk.data.len(),
                chunk_size,
                "Intermediate chunks should match chunk_size"
            );
        }

        let last = source.next_chunk().expect("last chunk must exist");
        assert_eq!(
            last.data.len(),
            512,
            "Last chunk should match the remainder size"
        );
        assert!(source.next_chunk().is_none(), "No extra chunks expected");
    }

    /// Validates the timeout constant is used correctly.
    ///
    /// This tests the fix for hung transfers where senders could wait indefinitely
    /// for receivers that never respond.
    #[test]
    fn transfer_timeout_constant_is_reasonable() {
        // 5 minutes = 300 seconds
        let timeout_duration = Duration::from_secs(TRANSFER_TIMEOUT_SECS);

        // Verify it's set to 5 minutes
        assert_eq!(
            timeout_duration.as_secs(),
            300,
            "Timeout should be 5 minutes (300 seconds)"
        );

        // Verify it's not too short (> 1 minute)
        assert!(
            timeout_duration.as_secs() > 60,
            "Timeout should be longer than 1 minute"
        );

        // Verify it's not too long (< 1 hour)
        assert!(
            timeout_duration.as_secs() < 3600,
            "Timeout should be shorter than 1 hour"
        );
    }

    #[test]
    fn fec_scheduler_plans_tree_unassigned_work_items() {
        let manifest = FecManifest::new_raptorq(2, 16);
        let mut scheduler = FecScheduler::new(manifest);
        let mut chunk_source = ChunkSource::new(Bytes::from(vec![0xAA; 32]), 16, 2, 32);

        let plan = scheduler
            .plan_next_block(42, &mut chunk_source)
            .expect("scheduler must plan block");
        let FecPlanResult::BlockQueued {
            block_id,
            source_symbols,
            repair_budget,
        } = plan
        else {
            panic!("expected queued block");
        };

        assert_eq!(block_id, 0, "first planned block id should be zero");
        assert_eq!(
            source_symbols, 2,
            "source symbol count should match chunk count"
        );
        assert_eq!(
            repair_budget, 1,
            "repair budget should follow current pre-T6 policy"
        );
        assert_eq!(
            scheduler.planned_block_count(),
            1,
            "planner cursor should advance after one block"
        );

        let first = scheduler
            .next_symbol_work_item()
            .expect("first symbol should exist");
        assert_eq!(first.block_id, 0);
        assert!(
            !first.is_repair,
            "systematic symbols should be emitted before repairs"
        );
    }

    #[test]
    fn fec_scheduler_tracks_repair_accounting() {
        let manifest = FecManifest::new_raptorq(4, 8);
        let mut scheduler = FecScheduler::new(manifest);
        let mut chunk_source = ChunkSource::new(Bytes::from(vec![0xCC; 32]), 8, 4, 32);

        let plan = scheduler
            .plan_next_block(7, &mut chunk_source)
            .expect("scheduler must plan block");
        let FecPlanResult::BlockQueued { repair_budget, .. } = plan else {
            panic!("expected queued block");
        };

        let mut repairs_observed = 0usize;
        while let Some(work_item) = scheduler.next_symbol_work_item() {
            if work_item.is_repair {
                repairs_observed = repairs_observed.saturating_add(1);
                scheduler.note_repair_symbol_sent(work_item.block_id);
            }
        }

        let stats = scheduler
            .block_stats(0)
            .expect("planned block stats should remain available");
        assert_eq!(repairs_observed, repair_budget);
        assert_eq!(stats.repairs_sent, repair_budget);
        assert_eq!(stats.repair_budget, repair_budget);
    }

    #[test]
    fn fec_scheduler_marks_source_drained_when_no_chunks_available() {
        let manifest = FecManifest::new_raptorq(2, 8);
        let mut scheduler = FecScheduler::new(manifest);
        let mut chunk_source = ChunkSource::new(Bytes::new(), 8, 0, 0);

        let plan = scheduler
            .plan_next_block(1, &mut chunk_source)
            .expect("empty source should not error");
        assert!(
            matches!(plan, FecPlanResult::SourceDrained),
            "scheduler should surface source-drained when no chunks remain"
        );
    }

    fn work_item(symbol_id: u32) -> FecSymbolWorkItem {
        FecSymbolWorkItem {
            block_id: 0,
            symbol_id,
            payload: Bytes::from_static(b"x"),
            is_repair: false,
        }
    }

    #[test]
    fn collaborative_dispatch_round_robins_across_lanes() {
        let (tx_a, mut rx_a) = mpsc::channel::<FecSymbolWorkItem>(2);
        let (tx_b, mut rx_b) = mpsc::channel::<FecSymbolWorkItem>(2);
        let outstanding = Arc::new(AtomicUsize::new(0));

        let mut dispatch = FecTreeDispatch {
            lanes: vec![
                FecTreeLane {
                    tree_id: 10,
                    tx: tx_a,
                },
                FecTreeLane {
                    tree_id: 20,
                    tx: tx_b,
                },
            ],
            next_rr_idx: 0,
            wakeup: Arc::new(Notify::new()),
            outstanding_symbols: Arc::clone(&outstanding),
            all_lanes_blocked: false,
        };

        let first = dispatch.try_enqueue(work_item(1));
        let second = dispatch.try_enqueue(work_item(2));
        let third = dispatch.try_enqueue(work_item(3));

        assert!(
            matches!(first, FecDispatchEnqueueResult::Queued { tree_id: 10 }),
            "first symbol should target the first lane"
        );
        assert!(
            matches!(second, FecDispatchEnqueueResult::Queued { tree_id: 20 }),
            "second symbol should target the second lane"
        );
        assert!(
            matches!(third, FecDispatchEnqueueResult::Queued { tree_id: 10 }),
            "round-robin cursor should wrap back to the first lane"
        );

        let lane_a_first = rx_a.try_recv().expect("lane A should have first symbol");
        let lane_b_first = rx_b.try_recv().expect("lane B should have second symbol");
        let lane_a_second = rx_a.try_recv().expect("lane A should have wrapped symbol");
        assert_eq!(lane_a_first.symbol_id, 1);
        assert_eq!(lane_b_first.symbol_id, 2);
        assert_eq!(lane_a_second.symbol_id, 3);
        assert_eq!(
            outstanding.load(Ordering::Relaxed),
            3,
            "dispatch should track outstanding queued symbols"
        );
    }

    #[test]
    fn collaborative_dispatch_skips_blocked_lane_immediately() {
        let (tx_a, mut rx_a) = mpsc::channel::<FecSymbolWorkItem>(1);
        let (tx_b, _rx_b) = mpsc::channel::<FecSymbolWorkItem>(1);
        let outstanding = Arc::new(AtomicUsize::new(0));

        tx_b.try_send(work_item(90))
            .expect("test setup should fill lane B");

        let mut dispatch = FecTreeDispatch {
            lanes: vec![
                FecTreeLane {
                    tree_id: 1,
                    tx: tx_a,
                },
                FecTreeLane {
                    tree_id: 2,
                    tx: tx_b,
                },
            ],
            next_rr_idx: 1,
            wakeup: Arc::new(Notify::new()),
            outstanding_symbols: Arc::clone(&outstanding),
            all_lanes_blocked: false,
        };

        let result = dispatch.try_enqueue(work_item(5));
        assert!(
            matches!(result, FecDispatchEnqueueResult::Queued { tree_id: 1 }),
            "dispatcher should skip blocked lane and enqueue on the first writable lane"
        );
        let delivered = rx_a
            .try_recv()
            .expect("writable lane should receive dispatched symbol");
        assert_eq!(delivered.symbol_id, 5);
        assert!(
            !dispatch.blocked_on_all_lanes(),
            "a successful enqueue must clear all-lanes-blocked state"
        );
    }

    #[test]
    fn collaborative_dispatch_reports_all_lanes_blocked() {
        let (tx_a, _rx_a) = mpsc::channel::<FecSymbolWorkItem>(1);
        let (tx_b, _rx_b) = mpsc::channel::<FecSymbolWorkItem>(1);
        let outstanding = Arc::new(AtomicUsize::new(0));

        tx_a.try_send(work_item(10))
            .expect("test setup should fill lane A");
        tx_b.try_send(work_item(11))
            .expect("test setup should fill lane B");

        let mut dispatch = FecTreeDispatch {
            lanes: vec![
                FecTreeLane {
                    tree_id: 3,
                    tx: tx_a,
                },
                FecTreeLane {
                    tree_id: 4,
                    tx: tx_b,
                },
            ],
            next_rr_idx: 0,
            wakeup: Arc::new(Notify::new()),
            outstanding_symbols: Arc::clone(&outstanding),
            all_lanes_blocked: false,
        };

        let result = dispatch.try_enqueue(work_item(12));
        assert!(
            matches!(result, FecDispatchEnqueueResult::AllBlocked { .. }),
            "dispatcher should surface all-lanes-blocked when every lane is full"
        );
        assert!(
            dispatch.blocked_on_all_lanes(),
            "all-lanes-blocked state should be latched for wakeup waits"
        );
    }

    fn sender_cfg(chunk_size: usize, bucket: Option<TokenBucketSpec>) -> SenderConfig {
        let common = CommonConfig {
            session_id: 1,
            dest_ip: Ipv4Addr::new(10, 0, 0, 2),
            chunk_size,
            src_port: 1000,
            dst_port: 2000,
            data_bucket: bucket,
            local_node_id: 1,
            user_space_base_addr: Ipv4Addr::new(10, 0, 0, 1),
            local_netmask: Ipv4Addr::new(255, 255, 255, 0),
        };

        SenderConfig {
            common,
            receiver_ids: vec![],
            total_bytes: 0,
            source_buffer: Bytes::new(),
            fec_manifest: None,
            fec_tree_ids: Vec::new(),
            fec_tree_lane_depth: 32,
            fec_dispatch_burst: 1,
            ready_grace_ms: 1,
            topology_ready: None,
        }
    }

    fn fec_sender_cfg(
        chunk_size: usize,
        total_bytes: u64,
        symbols_per_block: u16,
        receiver_ids: Vec<usize>,
    ) -> SenderConfig {
        let mut cfg = sender_cfg(chunk_size, None);
        cfg.total_bytes = total_bytes;
        cfg.source_buffer = Bytes::from(vec![0xAB; total_bytes as usize]);
        cfg.receiver_ids = receiver_ids;
        cfg.fec_manifest = Some(FecManifest::new_raptorq(
            symbols_per_block,
            chunk_size as u16,
        ));
        cfg.fec_tree_ids = vec![0, 1];
        cfg
    }

    fn fec_status(block_id: u64, deficit_symbols: u16) -> lossless_session::FecStatus {
        lossless_session::FecStatus {
            block_id,
            deficit_symbols,
        }
    }

    #[test]
    fn compute_window_defaults_to_constant_without_bucket() {
        let cfg = sender_cfg(1024, None);
        assert_eq!(compute_window(&cfg), DEFAULT_WINDOW);
    }

    #[test]
    fn compute_window_limits_to_bucket_capacity() {
        let bucket = TokenBucketSpec {
            rate: 10_000,
            bucket_size: 8_192,
        };
        let cfg = sender_cfg(2_048, Some(bucket));
        // bucket_size / chunk_size = 4, lower than DEFAULT_WINDOW
        assert_eq!(compute_window(&cfg), 4);
    }

    #[test]
    fn fec_status_advances_only_for_contiguous_completed_blocks() {
        let chunk_size = 8usize;
        let total_chunks = 3u64;
        let total_bytes = total_chunks * chunk_size as u64;
        let cfg = fec_sender_cfg(chunk_size, total_bytes, 1, vec![7]);
        let mut state = SenderState::new(cfg, total_chunks);
        if let Some(scheduler) = state.fec_scheduler.as_mut() {
            scheduler.next_block_id = total_chunks;
        }

        assert_eq!(state.retired_up_to, 0);

        let non_contiguous = state.update_receiver_fec_status(7, &fec_status(2, 0));
        assert!(
            non_contiguous.is_none(),
            "block-level status should not advance retire watermark across missing blocks"
        );
        state.update_retired_up_to();
        assert_eq!(state.retired_up_to, 0);

        let first = state.update_receiver_fec_status(7, &fec_status(0, 0));
        assert_eq!(
            first,
            Some(1),
            "first contiguous completed block should advance by one"
        );
        state.update_retired_up_to();
        assert_eq!(state.retired_up_to, 1);

        let second = state.update_receiver_fec_status(7, &fec_status(1, 0));
        assert_eq!(
            second,
            Some(3),
            "buffered out-of-order completion should flush contiguous progress"
        );
        state.update_retired_up_to();
        assert_eq!(state.retired_up_to, 3);
    }

    #[test]
    fn fec_completion_waits_for_lane_drain_under_asymmetric_pressure() {
        let chunk_size = 16usize;
        let total_chunks = 1u64;
        let total_bytes = total_chunks * chunk_size as u64;
        let cfg = fec_sender_cfg(chunk_size, total_bytes, 1, vec![11]);
        let mut state = SenderState::new(cfg, total_chunks);

        state.source_drained = true;
        if let Some(scheduler) = state.fec_scheduler.as_mut() {
            scheduler.next_block_id = 1;
        }
        state
            .update_receiver_fec_status(11, &fec_status(0, 0))
            .expect("receiver should report block completion");
        state.update_retired_up_to();

        let (tx_a, _rx_a) = mpsc::channel::<FecSymbolWorkItem>(1);
        let (tx_b, _rx_b) = mpsc::channel::<FecSymbolWorkItem>(1);
        let outstanding = Arc::new(AtomicUsize::new(1));
        state.fec_dispatch = Some(FecTreeDispatch {
            lanes: vec![
                FecTreeLane {
                    tree_id: 1,
                    tx: tx_a,
                },
                FecTreeLane {
                    tree_id: 2,
                    tx: tx_b,
                },
            ],
            next_rr_idx: 0,
            wakeup: Arc::new(Notify::new()),
            outstanding_symbols: Arc::clone(&outstanding),
            all_lanes_blocked: false,
        });

        assert_eq!(state.outstanding_units(), 0);
        assert!(
            !state.ready_to_emit_eot(),
            "EOT must wait for delayed lane drain even when block progress is complete"
        );

        outstanding.store(0, Ordering::Relaxed);
        assert!(
            state.ready_to_emit_eot(),
            "EOT should become eligible once all per-tree lane work is drained"
        );
    }

    #[test]
    fn fec_eot_last_index_uses_chunk_count() {
        let chunk_size = 1_024usize;
        let total_bytes = (chunk_size * 5) as u64;
        let mut cfg = sender_cfg(chunk_size, None);
        cfg.total_bytes = total_bytes;
        cfg.fec_manifest = Some(FecManifest::new_raptorq(2, chunk_size as u16));

        let total_chunks = total_bytes.div_ceil(chunk_size as u64);
        let state = SenderState::new(cfg, total_chunks);

        assert_eq!(
            state.eot_last_index(),
            total_chunks,
            "FEC EOT must use chunk index units so receiver completion checks remain correct"
        );
    }
}
