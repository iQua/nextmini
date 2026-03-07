use std::collections::{BTreeMap, BTreeSet, HashMap, VecDeque};
use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::Bytes;
use tokio::sync::{Mutex, mpsc};
use tokio::time::MissedTickBehavior;
use tracing::{debug, info, trace, warn};

use nextmini_messages::lossless_session::{
    self, FecManifest, LOSSLESS_SESSION_BASE_VERSION, LOSSLESS_SESSION_FEC_VERSION,
    LosslessSessionControl,
};

use crate::node::packet::Packet;
use crate::node::processor::ProcessorHandle;
use crate::node::session::api::InboundFrame;
use crate::node::session::fec::{self, BlockParams};
use crate::node::session::runtime::ReceiverConfig;
use crate::node::{NodeId, NodeIdExt};

const ACK_EVERY_CHUNKS: u64 = 16;
const FEC_FEEDBACK_INTERVAL: Duration = Duration::from_millis(20);
const FEC_FEEDBACK_MAX_JITTER_MS: u64 = 11;
const FEC_DECODED_HISTORY_LEN: usize = 1024;

/// Utility for emitting completion control traffic (ACKs) via the node
/// processor stack using the same addressing the sender expects.
struct ControlEmitter {
    session_id: u64,
    src_ip: std::net::Ipv4Addr,
    src_port: u16,
    dst_ip: std::net::Ipv4Addr,
    dst_port: u16,
    processors: ProcessorHandle,
}

impl ControlEmitter {
    /// Prepare an emitter that can forward lossless session control traffic back through the
    /// node's processor pipeline.
    fn new(
        session_id: u64,
        src_ip: std::net::Ipv4Addr,
        src_port: u16,
        dst_ip: std::net::Ipv4Addr,
        dst_port: u16,
        processors: ProcessorHandle,
    ) -> Self {
        Self {
            session_id,
            src_ip,
            src_port,
            dst_ip,
            dst_port,
            processors,
        }
    }

    /// Encode and inject a single control frame.
    async fn send(&self, control: &LosslessSessionControl) {
        self.send_with_version(control, LOSSLESS_SESSION_BASE_VERSION)
            .await;
    }

    async fn send_with_version(&self, control: &LosslessSessionControl, version: u8) {
        // Use stack-allocated buffer to avoid heap allocation for small control frames
        let mut buf = [0u8; lossless_session::MAX_CONTROL_FRAME_SIZE];
        let frame = lossless_session::encode_control_into_with_version(
            &mut buf,
            self.session_id,
            version,
            control,
        );

        let packet = Packet::build_ipv4_tcp_packet(
            self.src_ip,
            self.src_port,
            self.dst_ip,
            self.dst_port,
            frame,
        );

        self.processors.process_packet(packet).await;
    }
}

/// Drives a receiver session: consumes inbound frames, persists payloads in
/// order, and sends acknowledgement signals back to the sender.
pub async fn run(
    cfg: ReceiverConfig,
    mut rx: mpsc::Receiver<InboundFrame>,
    processors: ProcessorHandle,
) {
    let sid = cfg.common.session_id;
    info!(
        session_id = sid,
        expected_bytes = cfg.expected_bytes,
        "Lossless receiver started"
    );

    // Stream bookkeeping: lossless session chunk indices start at 1.
    let mut expected: u64 = 1;
    let mut pending: HashMap<u64, Bytes> = HashMap::new();
    let mut bytes_received: u64 = 0;
    let sink_buffer = cfg.sink_buffer.clone();

    let src_ip = (cfg.common.local_node_id as NodeId)
        .ip_addr(cfg.common.user_space_base_addr, cfg.common.local_netmask);
    let dst_ip = (cfg.source_node_id as NodeId)
        .ip_addr(cfg.common.user_space_base_addr, cfg.common.local_netmask);

    // Source control traffic from the client (src) port to match sender expectations.
    let ctrl_src_port = cfg.common.src_port;
    let ctrl_dst_port = cfg.common.dst_port;

    let control_io = ControlEmitter::new(
        sid,
        src_ip,
        ctrl_src_port,
        dst_ip,
        ctrl_dst_port,
        processors.clone(),
    );

    control_io
        .send(&LosslessSessionControl::Ready {
            node_id: cfg.common.local_node_id as u64,
        })
        .await;
    let mut last_ack_up_to: u64 = 0;
    let mut eot_index: Option<u64> = None;
    let mut fec_manifest: Option<FecManifest> = None;
    let mut fec_state: Option<FecReceiverState> = None;
    let mut terminal_feedback_tick = tokio::time::interval(FEC_FEEDBACK_INTERVAL);
    terminal_feedback_tick.set_missed_tick_behavior(MissedTickBehavior::Skip);
    terminal_feedback_tick.tick().await;

    loop {
        tokio::select! {
            maybe_frame = rx.recv() => {
                let Some(frame) = maybe_frame else {
                    break;
                };
                trace!(
                    session_id = sid,
                    frame_len = frame.bytes.len(),
                    "Lossless receiver: received inbound frame"
                );
                if let Some((_, data, body)) = lossless_session::decode_data(&frame.bytes) {
                    let payload_range = {
                        let base_ptr = frame.bytes.as_ptr() as usize;
                        let start = body.as_ptr() as usize - base_ptr;
                        let end = start + body.len();
                        start..end
                    };
                    process_decoded_data_frame(
                        sid,
                        &cfg,
                        &control_io,
                        &sink_buffer,
                        data,
                        frame.bytes,
                        payload_range,
                        &mut expected,
                        &mut pending,
                        &mut bytes_received,
                        &mut last_ack_up_to,
                        eot_index,
                    )
                    .await;
                    continue;
                }

                if let Some((_, fec_data, body)) = lossless_session::decode_fec_data(&frame.bytes) {
                    let Some(manifest) = fec_manifest else {
                        warn!(
                            session_id = sid,
                            block_id = fec_data.block_id,
                            symbol_id = fec_data.symbol_id,
                            "Lossless receiver: dropping FEC data before receiving FEC manifest"
                        );
                        continue;
                    };
                    if !cfg.fec_capabilities.supports_manifest(&manifest) {
                        warn!(
                            session_id = sid,
                            scheme = manifest.scheme,
                            "Lossless receiver: dropping FEC data for unsupported manifest"
                        );
                        continue;
                    }
                    if let Some(state) = fec_state.as_ref()
                        && (state.symbols_per_block != manifest.symbols_per_block.max(1)
                            || state.symbol_size != usize::from(manifest.symbol_size.max(1)))
                    {
                        fec_state = None;
                    }
                    let state = fec_state.get_or_insert_with(|| {
                        FecReceiverState::new(sid, cfg.common.local_node_id, manifest, &cfg)
                    });
                    let payload_range = {
                        let base_ptr = frame.bytes.as_ptr() as usize;
                        let start = body.as_ptr() as usize - base_ptr;
                        let end = start + body.len();
                        start..end
                    };
                    process_fec_data_frame(
                        sid,
                        &cfg,
                        &control_io,
                        &sink_buffer,
                        fec_data,
                        manifest,
                        frame.bytes,
                        payload_range,
                        state,
                        &mut expected,
                        &mut pending,
                        &mut bytes_received,
                    )
                    .await;
                    continue;
                }

                if handle_control_frame(&frame, &cfg, &control_io, &mut eot_index, &mut fec_manifest).await
                {
                    if let Some(last) = eot_index
                        && expected.saturating_sub(1) >= last
                    {
                        break;
                    }
                    continue;
                }

                warn!(
                    session_id = sid,
                    "Lossless receiver: received frame that was neither DATA nor CONTROL"
                );
            }
            _ = terminal_feedback_tick.tick() => {
                let Some(state) = fec_state.as_mut() else {
                    continue;
                };

                for status in state.take_due_terminal_feedback(Instant::now()) {
                    debug!(
                        session_id = sid,
                        block_id = status.block_id,
                        "Lossless receiver: re-sending terminal FEC status update"
                    );
                    control_io
                        .send_with_version(
                            &LosslessSessionControl::FecStatus { status },
                            LOSSLESS_SESSION_FEC_VERSION,
                        )
                        .await;
                }
            }
        }
    }

    if bytes_received != cfg.expected_bytes {
        warn!(
            session_id = sid,
            bytes_received,
            expected_bytes = cfg.expected_bytes,
            "Lossless receiver: object integrity check failed (size mismatch)"
        );
    }
    if let Some(state) = fec_state.as_ref()
        && state.integrity_error
    {
        warn!(
            session_id = sid,
            "Lossless receiver: object integrity check observed malformed FEC symbols"
        );
    }

    info!(
        session_id = sid,
        bytes_received,
        last_index = expected.saturating_sub(1),
        "Lossless receiver finished"
    );
}

#[allow(clippy::too_many_arguments)]
async fn process_decoded_data_frame(
    sid: u64,
    cfg: &ReceiverConfig,
    control_io: &ControlEmitter,
    sink_buffer: &Option<Arc<Mutex<Vec<u8>>>>,
    data: lossless_session::LosslessSessionData,
    frame_bytes: Vec<u8>,
    payload_range: std::ops::Range<usize>,
    expected: &mut u64,
    pending: &mut HashMap<u64, Bytes>,
    bytes_received: &mut u64,
    last_ack_up_to: &mut u64,
    eot_index: Option<u64>,
) {
    let frame_bytes = Bytes::from(frame_bytes);
    let payload = frame_bytes.slice(payload_range);

    let ctx = FrameCtx {
        data: &data,
        payload,
        expected,
        pending,
        bytes_received,
    };
    let outcome = handle_data_frame(ctx);
    if !outcome.ready_chunks.is_empty()
        && let Some(buf) = sink_buffer
    {
        let mut guard = buf.lock().await;
        for chunk in &outcome.ready_chunks {
            guard.extend_from_slice(chunk);
        }
    }
    if !outcome.advanced {
        return;
    }

    let base = expected.saturating_sub(1);
    if base <= *last_ack_up_to {
        return;
    }

    let advanced_chunks = base - *last_ack_up_to;
    let final_chunk_reached = matches!(eot_index, Some(last) if last == base);
    let received_all_bytes = *bytes_received >= cfg.expected_bytes;
    if advanced_chunks < ACK_EVERY_CHUNKS && !final_chunk_reached && !received_all_bytes {
        return;
    }

    debug!(
        session_id = sid,
        up_to = base,
        expected = expected,
        "Lossless receiver: sending batched ACK"
    );
    control_io
        .send(&LosslessSessionControl::Ack { up_to: base })
        .await;
    *last_ack_up_to = base;
}

#[allow(clippy::too_many_arguments)]
async fn process_fec_data_frame(
    sid: u64,
    cfg: &ReceiverConfig,
    control_io: &ControlEmitter,
    sink_buffer: &Option<Arc<Mutex<Vec<u8>>>>,
    fec_data: lossless_session::LosslessSessionFecData,
    manifest: FecManifest,
    frame_bytes: Vec<u8>,
    payload_range: std::ops::Range<usize>,
    fec_state: &mut FecReceiverState,
    expected: &mut u64,
    pending: &mut HashMap<u64, Bytes>,
    bytes_received: &mut u64,
) {
    let frame_bytes = Bytes::from(frame_bytes);
    let payload = frame_bytes.slice(payload_range);
    if payload.len() != fec_data.payload_len as usize {
        warn!(
            session_id = sid,
            block_id = fec_data.block_id,
            symbol_id = fec_data.symbol_id,
            advertised_payload_len = fec_data.payload_len,
            actual_payload_len = payload.len(),
            "Lossless receiver: FEC symbol payload length mismatch"
        );
    }

    if manifest.symbol_size == 0 {
        warn!(
            session_id = sid,
            block_id = fec_data.block_id,
            "Lossless receiver: dropping FEC symbol with zero symbol_size manifest"
        );
        return;
    }

    let outcome = fec_state.ingest_symbol(
        fec_data.block_id,
        fec_data.symbol_id,
        payload.as_ref(),
        Instant::now(),
    );

    if let Some(status) = outcome.feedback {
        debug!(
            session_id = sid,
            block_id = status.block_id,
            deficit_symbols = status.deficit_symbols,
            "Lossless receiver: sending bounded FEC status update"
        );
        control_io
            .send_with_version(
                &LosslessSessionControl::FecStatus { status },
                LOSSLESS_SESSION_FEC_VERSION,
            )
            .await;
    }

    if outcome.decoded_chunks.is_empty() {
        return;
    }

    let mut ready_chunks: Vec<Bytes> = Vec::new();
    for decoded in outcome.decoded_chunks {
        let data = lossless_session::LosslessSessionData {
            index: decoded.index,
            payload_len: decoded.payload.len() as u32,
        };
        let ctx = FrameCtx {
            data: &data,
            payload: decoded.payload,
            expected,
            pending,
            bytes_received,
        };
        let partial = handle_data_frame(ctx);
        ready_chunks.extend(partial.ready_chunks);
    }

    if !ready_chunks.is_empty()
        && let Some(buf) = sink_buffer
    {
        let mut guard = buf.lock().await;
        for chunk in ready_chunks {
            guard.extend_from_slice(&chunk);
        }
    }

    if *bytes_received >= cfg.expected_bytes {
        trace!(
            session_id = sid,
            bytes_received = *bytes_received,
            "Lossless receiver: reconstructed expected payload bytes via FEC"
        );
    }
}

struct FecDecodedChunk {
    index: u64,
    payload: Bytes,
}

struct FecIngestOutcome {
    decoded_chunks: Vec<FecDecodedChunk>,
    feedback: Option<lossless_session::FecStatus>,
}

struct FecReceiverState {
    session_id: u64,
    local_node_id: usize,
    symbols_per_block: u16,
    symbol_size: usize,
    chunk_size: usize,
    expected_bytes: u64,
    total_chunks: u64,
    blocks: BTreeMap<u64, FecBlockState>,
    decoded_recent: VecDeque<u64>,
    decoded_set: BTreeSet<u64>,
    terminal_feedback_resend_at: BTreeMap<u64, Instant>,
    integrity_error: bool,
}

impl FecReceiverState {
    fn new(
        session_id: u64,
        local_node_id: usize,
        manifest: FecManifest,
        cfg: &ReceiverConfig,
    ) -> Self {
        let chunk_size = cfg.common.chunk_size.max(1);
        let expected_bytes = cfg.expected_bytes;
        let total_chunks = if expected_bytes == 0 {
            0
        } else {
            expected_bytes.div_ceil(chunk_size as u64)
        };

        Self {
            session_id,
            local_node_id,
            symbols_per_block: manifest.symbols_per_block.max(1),
            symbol_size: usize::from(manifest.symbol_size.max(1)),
            chunk_size,
            expected_bytes,
            total_chunks,
            blocks: BTreeMap::new(),
            decoded_recent: VecDeque::new(),
            decoded_set: BTreeSet::new(),
            terminal_feedback_resend_at: BTreeMap::new(),
            integrity_error: false,
        }
    }

    fn ingest_symbol(
        &mut self,
        block_id: u64,
        symbol_id: u32,
        payload: &[u8],
        now: Instant,
    ) -> FecIngestOutcome {
        if self.decoded_set.contains(&block_id) {
            return FecIngestOutcome {
                decoded_chunks: Vec::new(),
                feedback: None,
            };
        }

        if !self.is_valid_block_id(block_id) {
            warn!(
                session_id = self.session_id,
                block_id,
                total_chunks = self.total_chunks,
                symbols_per_block = self.symbols_per_block,
                "Lossless receiver: dropping FEC symbol for unexpected block"
            );
            return FecIngestOutcome {
                decoded_chunks: Vec::new(),
                feedback: None,
            };
        }

        let params = self.block_params(block_id);
        let first_feedback_at = now + feedback_jitter(self.local_node_id, block_id);

        let mut decoded_symbols: Option<Vec<Vec<u8>>> = None;
        let payload_malformed;
        let mut decoded_block = false;
        let feedback = {
            let block = self
                .blocks
                .entry(block_id)
                .or_insert_with(|| FecBlockState::new(params, first_feedback_at));

            block.ingest_symbol(symbol_id, payload, self.symbol_size);
            payload_malformed = block.payload_malformed;

            let mut deficit = block.deficit(self.symbols_per_block);
            if !block.decoded && deficit == 0 {
                match block.decode_symbols(self.symbols_per_block as usize) {
                    Ok(decoded) => {
                        block.decoded = true;
                        block.received.clear();
                        deficit = 0;
                        decoded_block = true;
                        decoded_symbols = Some(decoded.source_symbols);
                    }
                    Err(err) => {
                        // Keep asking for one more symbol if rank was insufficient even after K inputs.
                        deficit = 1;
                        debug!(
                            block_id,
                            symbol_count = block.received.len(),
                            ?err,
                            "Lossless receiver: FEC decode attempt needs additional symbols"
                        );
                    }
                }
            }

            block.maybe_feedback(block_id, deficit, now)
        };
        if decoded_block {
            self.blocks.remove(&block_id);
            self.mark_block_decoded(block_id);
            self.arm_terminal_feedback(block_id, now);
        }

        if payload_malformed {
            self.integrity_error = true;
        }

        let mut decoded_chunks = Vec::new();
        if let Some(source_symbols) = decoded_symbols {
            let symbols_per_block = u64::from(self.symbols_per_block);
            for (esi, source_symbol) in source_symbols.into_iter().enumerate() {
                let index = block_id
                    .saturating_mul(symbols_per_block)
                    .saturating_add(esi as u64)
                    .saturating_add(1);
                if index == 0 || index > self.total_chunks {
                    continue;
                }
                let chunk_len = self.chunk_len_for_index(index);
                if chunk_len > source_symbol.len() {
                    self.integrity_error = true;
                    continue;
                }
                decoded_chunks.push(FecDecodedChunk {
                    index,
                    payload: Bytes::from(source_symbol[..chunk_len].to_vec()),
                });
            }
        }

        FecIngestOutcome {
            decoded_chunks,
            feedback,
        }
    }

    fn block_params(&self, block_id: u64) -> BlockParams {
        BlockParams::new(
            usize::from(self.symbols_per_block),
            self.symbol_size,
            fec::block_seed(self.session_id, block_id),
        )
    }

    fn mark_block_decoded(&mut self, block_id: u64) {
        if !self.decoded_set.insert(block_id) {
            return;
        }
        self.decoded_recent.push_back(block_id);
        while self.decoded_recent.len() > FEC_DECODED_HISTORY_LEN {
            if let Some(old) = self.decoded_recent.pop_front() {
                self.decoded_set.remove(&old);
                self.terminal_feedback_resend_at.remove(&old);
            }
        }
    }

    fn arm_terminal_feedback(&mut self, block_id: u64, now: Instant) {
        self.terminal_feedback_resend_at
            .insert(block_id, now + FEC_FEEDBACK_INTERVAL);
    }

    fn take_due_terminal_feedback(&mut self, now: Instant) -> Vec<lossless_session::FecStatus> {
        let mut due = Vec::new();
        for (block_id, resend_at) in &mut self.terminal_feedback_resend_at {
            if now < *resend_at {
                continue;
            }
            due.push(lossless_session::FecStatus {
                block_id: *block_id,
                deficit_symbols: 0,
            });
            *resend_at = now + FEC_FEEDBACK_INTERVAL;
        }
        due
    }

    fn total_fec_blocks(&self) -> u64 {
        if self.total_chunks == 0 {
            0
        } else {
            self.total_chunks.div_ceil(self.symbols_per_block as u64)
        }
    }

    fn is_valid_block_id(&self, block_id: u64) -> bool {
        let total_blocks = self.total_fec_blocks();
        if total_blocks == 0 {
            return false;
        }
        block_id < total_blocks
    }

    fn chunk_len_for_index(&self, index: u64) -> usize {
        if index == 0 || index > self.total_chunks {
            return 0;
        }
        if index < self.total_chunks {
            return self.chunk_size;
        }

        let rem = (self.expected_bytes % self.chunk_size as u64) as usize;
        if rem == 0 { self.chunk_size } else { rem }
    }
}

struct FecBlockState {
    decoder: fec::Decoder,
    received: BTreeMap<u32, Vec<u8>>,
    decoded: bool,
    next_feedback_at: Instant,
    last_feedback_deficit: Option<u16>,
    payload_malformed: bool,
}

impl FecBlockState {
    fn new(params: BlockParams, next_feedback_at: Instant) -> Self {
        Self {
            decoder: fec::Decoder::from_block(params),
            received: BTreeMap::new(),
            decoded: false,
            next_feedback_at,
            last_feedback_deficit: None,
            payload_malformed: false,
        }
    }

    fn ingest_symbol(&mut self, symbol_id: u32, payload: &[u8], symbol_size: usize) {
        if self.decoded || self.received.contains_key(&symbol_id) {
            return;
        }
        if payload.len() > symbol_size {
            self.payload_malformed = true;
            return;
        }

        let mut normalized = vec![0u8; symbol_size];
        normalized[..payload.len()].copy_from_slice(payload);
        self.received.insert(symbol_id, normalized);
    }

    fn deficit(&self, symbols_per_block: u16) -> u16 {
        let required = usize::from(symbols_per_block.max(1));
        let have = self.received.len().min(required);
        (required - have) as u16
    }

    fn decode_symbols(&self, source_symbols: usize) -> Result<fec::DecodeOutput, fec::DecodeError> {
        let mut symbols = self.decoder.constraint_symbols();
        symbols.reserve(self.received.len());
        for (esi, payload) in &self.received {
            let symbol = if (*esi as usize) < source_symbols {
                self.decoder.source_symbol(*esi, payload.clone())
            } else {
                self.decoder.coded_symbol(*esi, payload.clone())
            };
            symbols.push(symbol);
        }
        self.decoder.decode(&symbols)
    }

    fn maybe_feedback(
        &mut self,
        block_id: u64,
        deficit: u16,
        now: Instant,
    ) -> Option<lossless_session::FecStatus> {
        let terminal = deficit == 0 && self.last_feedback_deficit != Some(0);
        let changed = self.last_feedback_deficit != Some(deficit);

        if !terminal && now < self.next_feedback_at {
            return None;
        }
        if !terminal && !changed && deficit != 0 {
            return None;
        }

        self.last_feedback_deficit = Some(deficit);
        self.next_feedback_at = now + FEC_FEEDBACK_INTERVAL;

        Some(lossless_session::FecStatus {
            block_id,
            deficit_symbols: deficit,
        })
    }
}

fn feedback_jitter(local_node_id: usize, block_id: u64) -> Duration {
    let spread = (local_node_id as u64)
        .wrapping_mul(17)
        .wrapping_add(block_id.wrapping_mul(31))
        % FEC_FEEDBACK_MAX_JITTER_MS.max(1);
    Duration::from_millis(spread)
}

/// Drain contiguous payloads starting at `*expected` from the pending map,
/// advancing the expected index as chunks are consumed.
fn drain_contiguous(pending: &mut HashMap<u64, Bytes>, expected: &mut u64) -> Vec<Bytes> {
    let mut ready = Vec::new();
    while let Some(payload) = pending.remove(expected) {
        ready.push(payload);
        *expected += 1;
    }
    ready
}

/// Borrowed state required to evaluate a DATA frame.
struct FrameCtx<'a> {
    data: &'a lossless_session::LosslessSessionData,
    payload: Bytes,
    expected: &'a mut u64,
    pending: &'a mut HashMap<u64, Bytes>,
    bytes_received: &'a mut u64,
}

/// Outcome describing whether the new frame unlocked bytes for delivery.
struct DataOutcome {
    ready_chunks: Vec<Bytes>,
    advanced: bool,
}

/// Handles ordering/bookkeeping for a single lossless DATA frame.
fn handle_data_frame(ctx: FrameCtx<'_>) -> DataOutcome {
    let FrameCtx {
        data,
        payload,
        expected,
        pending,
        bytes_received,
    } = ctx;

    let idx = data.index;

    if idx < *expected {
        return DataOutcome {
            ready_chunks: Vec::new(),
            advanced: false,
        };
    }

    use std::collections::hash_map::Entry;
    if let Entry::Vacant(e) = pending.entry(idx) {
        e.insert(payload);
        trace!(
            chunk_index = idx,
            "Lossless receiver: chunk stored for ordering"
        );
    }

    let ready_chunks = drain_contiguous(pending, expected);
    if !ready_chunks.is_empty() {
        let ready_bytes: u64 = ready_chunks.iter().map(|chunk| chunk.len() as u64).sum();
        *bytes_received += ready_bytes;
    }

    let advanced = !ready_chunks.is_empty();

    DataOutcome {
        ready_chunks,
        advanced,
    }
}

/// Handles receiver-side control frames (Manifest/EOT/etc.).
async fn handle_control_frame(
    frame: &InboundFrame,
    cfg: &ReceiverConfig,
    ctrl_io: &ControlEmitter,
    eot_index: &mut Option<u64>,
    fec_manifest: &mut Option<FecManifest>,
) -> bool {
    let Some((_, control)) = lossless_session::decode_control(&frame.bytes) else {
        return false;
    };
    match control {
        LosslessSessionControl::Manifest { .. } => {
            ctrl_io
                .send(&LosslessSessionControl::Ready {
                    node_id: cfg.common.local_node_id as u64,
                })
                .await;
            true
        }
        LosslessSessionControl::FecManifest { fec, .. } => {
            *fec_manifest = Some(fec);

            if fec.scheme_kind().is_none() {
                warn!(
                    session_id = cfg.common.session_id,
                    scheme = fec.scheme,
                    "Lossless receiver: received unknown FEC scheme"
                );
            }

            ctrl_io
                .send_with_version(
                    &LosslessSessionControl::FecCapabilities {
                        node_id: cfg.common.local_node_id as u64,
                        capabilities: cfg.fec_capabilities,
                    },
                    LOSSLESS_SESSION_FEC_VERSION,
                )
                .await;
            ctrl_io
                .send_with_version(
                    &LosslessSessionControl::Ready {
                        node_id: cfg.common.local_node_id as u64,
                    },
                    LOSSLESS_SESSION_FEC_VERSION,
                )
                .await;
            true
        }
        LosslessSessionControl::Eot { last_index } => {
            info!(
                session_id = cfg.common.session_id,
                last_index = last_index,
                "Lossless receiver: EOT received"
            );
            *eot_index = Some(last_index);
            true
        }
        _ => true,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use nextmini_messages::lossless_session::FecManifest;

    #[test]
    fn drain_contiguous_in_order() {
        let mut pending = HashMap::new();
        let mut expected = 1u64;

        for i in 1..=5 {
            pending.insert(i, Bytes::from(vec![i as u8; 100]));
        }

        let ready = drain_contiguous(&mut pending, &mut expected);
        assert_eq!(ready.len(), 5);
        assert_eq!(expected, 6);
    }

    #[test]
    fn drain_contiguous_gap_then_fill() {
        let mut pending = HashMap::new();
        let mut expected = 1u64;

        pending.insert(10, Bytes::from(vec![10u8; 100]));

        let ready = drain_contiguous(&mut pending, &mut expected);
        assert_eq!(ready.len(), 0);
        assert_eq!(expected, 1);

        for i in 1..=9 {
            pending.insert(i, Bytes::from(vec![i as u8; 100]));
        }

        let ready = drain_contiguous(&mut pending, &mut expected);
        assert_eq!(ready.len(), 10);
        assert_eq!(expected, 11);
    }

    #[test]
    fn pending_accepts_any_offset() {
        let mut pending = HashMap::new();
        pending.insert(1_000_000u64, Bytes::from(vec![1u8; 100]));
        assert_eq!(pending.len(), 1);
    }

    #[test]
    fn fec_feedback_is_throttled_and_terminal_is_immediate() {
        let params = BlockParams::new(4, 32, 7);
        let t0 = Instant::now();
        let mut block = FecBlockState::new(params, t0 + Duration::from_millis(5));

        assert!(block.maybe_feedback(3, 4, t0).is_none());

        let first = block
            .maybe_feedback(3, 4, t0 + Duration::from_millis(5))
            .expect("feedback should be sent after jitter");
        assert_eq!(first.block_id, 3);
        assert_eq!(first.deficit_symbols, 4);

        assert!(
            block
                .maybe_feedback(3, 2, t0 + Duration::from_millis(10))
                .is_none(),
            "deficit updates are throttled within the interval"
        );

        let second = block
            .maybe_feedback(3, 2, t0 + Duration::from_millis(30))
            .expect("feedback should be re-eligible after throttle interval");
        assert_eq!(second.deficit_symbols, 2);

        let terminal = block
            .maybe_feedback(3, 0, t0 + Duration::from_millis(31))
            .expect("terminal completion should bypass throttle");
        assert_eq!(terminal.deficit_symbols, 0);

        assert!(
            block
                .maybe_feedback(3, 0, t0 + Duration::from_millis(50))
                .is_none(),
            "terminal completion should still respect the resend interval"
        );

        let resend = block
            .maybe_feedback(3, 0, t0 + Duration::from_millis(51))
            .expect("terminal completion should be re-advertised periodically");
        assert_eq!(resend.deficit_symbols, 0);
    }

    #[test]
    fn fec_receiver_state_re_advertises_decoded_block_completion() {
        let cfg = ReceiverConfig {
            common: crate::node::session::runtime::CommonConfig {
                session_id: 12,
                dest_ip: std::net::Ipv4Addr::new(10, 0, 0, 2),
                chunk_size: 8,
                src_port: 2000,
                dst_port: 3000,
                data_bucket: None,
                local_node_id: 1,
                user_space_base_addr: std::net::Ipv4Addr::new(10, 0, 0, 0),
                local_netmask: std::net::Ipv4Addr::new(255, 255, 255, 0),
            },
            source_node_id: 2,
            expected_bytes: 16,
            sink_buffer: None,
            fec_capabilities: nextmini_messages::lossless_session::FecCapabilities::default(),
        };

        let mut state = FecReceiverState::new(12, 1, FecManifest::new_raptorq(2, 8), &cfg);
        let t0 = Instant::now();

        assert!(state.ingest_symbol(0, 0, &[1u8; 8], t0).feedback.is_none());

        let t1 = t0 + Duration::from_millis(1);
        let completion = state.ingest_symbol(0, 1, &[2u8; 8], t1);
        let status = completion
            .feedback
            .expect("block completion should emit terminal feedback immediately");
        assert_eq!(status.block_id, 0);
        assert_eq!(status.deficit_symbols, 0);

        assert!(
            state
                .take_due_terminal_feedback(t1 + Duration::from_millis(19))
                .is_empty(),
            "terminal feedback should wait for resend interval"
        );
        let resend = state.take_due_terminal_feedback(t1 + Duration::from_millis(20));
        assert_eq!(resend.len(), 1);
        assert_eq!(resend[0].block_id, 0);
        assert_eq!(resend[0].deficit_symbols, 0);
    }

    #[test]
    fn fec_receiver_state_trims_last_chunk_to_expected_bytes() {
        let cfg = ReceiverConfig {
            common: crate::node::session::runtime::CommonConfig {
                session_id: 9,
                dest_ip: std::net::Ipv4Addr::new(10, 0, 0, 2),
                chunk_size: 8,
                src_port: 2000,
                dst_port: 3000,
                data_bucket: None,
                local_node_id: 1,
                user_space_base_addr: std::net::Ipv4Addr::new(10, 0, 0, 0),
                local_netmask: std::net::Ipv4Addr::new(255, 255, 255, 0),
            },
            source_node_id: 2,
            expected_bytes: 18,
            sink_buffer: None,
            fec_capabilities: nextmini_messages::lossless_session::FecCapabilities::default(),
        };

        let state = FecReceiverState::new(9, 1, FecManifest::new_raptorq(4, 8), &cfg);
        assert_eq!(state.total_chunks, 3);
        assert_eq!(state.chunk_len_for_index(1), 8);
        assert_eq!(state.chunk_len_for_index(2), 8);
        assert_eq!(state.chunk_len_for_index(3), 2);
    }

    #[test]
    fn fec_receiver_state_ignores_late_symbols_after_decode() {
        let cfg = ReceiverConfig {
            common: crate::node::session::runtime::CommonConfig {
                session_id: 11,
                dest_ip: std::net::Ipv4Addr::new(10, 0, 0, 2),
                chunk_size: 8,
                src_port: 2000,
                dst_port: 3000,
                data_bucket: None,
                local_node_id: 0,
                user_space_base_addr: std::net::Ipv4Addr::new(10, 0, 0, 0),
                local_netmask: std::net::Ipv4Addr::new(255, 255, 255, 0),
            },
            source_node_id: 2,
            expected_bytes: 16,
            sink_buffer: None,
            fec_capabilities: nextmini_messages::lossless_session::FecCapabilities::default(),
        };

        let mut state = FecReceiverState::new(11, 0, FecManifest::new_raptorq(2, 8), &cfg);
        let t0 = Instant::now();

        let first = state.ingest_symbol(0, 0, &[1u8; 8], t0);
        assert!(first.decoded_chunks.is_empty());

        let second = state.ingest_symbol(0, 1, &[2u8; 8], t0 + Duration::from_millis(1));
        assert_eq!(second.decoded_chunks.len(), 2);

        let late = state.ingest_symbol(0, 2, &[3u8; 8], t0 + Duration::from_millis(2));
        assert!(late.decoded_chunks.is_empty());
        assert!(late.feedback.is_none());
    }
}
