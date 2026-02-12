use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::Bytes;
use tokio::sync::{Mutex, mpsc};
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

const ACK_EVERY_CHUNKS: u64 = 16; // ensure <= sender DEFAULT_WINDOW
const FEC_FEEDBACK_INTERVAL: Duration = Duration::from_millis(20);
const FEC_FEEDBACK_MAX_JITTER_MS: u64 = 11;

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
    let per_chunk = cfg.common.chunk_size.max(1);

    // Uses a sliding window size corresponding to the burst size in the token bucket
    // If the token bucket shaper is not configured, use the default window size
    let window_size = cfg
        .common
        .data_bucket
        .as_ref()
        .map(|bucket| (bucket.bucket_size / per_chunk).max(1))
        .unwrap_or(super::sender::DEFAULT_WINDOW);

    let mut pending = PendingWindow::new(window_size, expected);
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

    while let Some(frame) = rx.recv().await {
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
    pending: &mut PendingWindow,
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
    pending: &mut PendingWindow,
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
        let params = self.block_params(block_id);
        let first_feedback_at = now + feedback_jitter(self.local_node_id, block_id);

        let mut decoded_symbols: Option<Vec<Vec<u8>>> = None;
        let payload_malformed;
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
            fec_block_seed(self.session_id, block_id),
        )
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
                self.decoder.repair_symbol(*esi, payload.clone())
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
        if !terminal && !changed {
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

fn fec_block_seed(session_id: u64, block_id: u64) -> u64 {
    session_id.wrapping_mul(0x9E37_79B9_7F4A_7C15) ^ block_id.wrapping_mul(0xBF58_476D_1CE4_E5B9)
}

fn feedback_jitter(local_node_id: usize, block_id: u64) -> Duration {
    let spread = (local_node_id as u64)
        .wrapping_mul(17)
        .wrapping_add(block_id.wrapping_mul(31))
        % FEC_FEEDBACK_MAX_JITTER_MS.max(1);
    Duration::from_millis(spread)
}

/// Fixed-size buffer that keeps track of out-of-order chunks within the current
/// receiver window.
struct PendingWindow {
    base_index: u64,
    head: usize,
    slots: Vec<Option<Bytes>>,
}

impl PendingWindow {
    /// Create a pending window sized to the configured sliding window.
    fn new(window_size: usize, base_index: u64) -> Self {
        let size = window_size.max(1);
        Self {
            base_index,
            head: 0,
            slots: vec![None; size],
        }
    }

    /// Attempt to store a chunk for later delivery; returns true if it landed in
    /// the buffer and false if it was out of range or a duplicate.
    fn insert(&mut self, index: u64, payload: Bytes) -> bool {
        if index < self.base_index {
            return false;
        }
        let offset = index - self.base_index;
        if offset >= self.slots.len() as u64 {
            warn!(
                chunk_index = index,
                base_index = self.base_index,
                window = self.slots.len(),
                "Lossless receiver: chunk outside pending window, dropping"
            );
            return false;
        }
        let slot_idx = self.slot_index(offset);
        if self.slots[slot_idx].is_none() {
            self.slots[slot_idx] = Some(payload);
            true
        } else {
            false
        }
    }

    /// Drain any contiguous payloads starting at `expected`, advancing the base
    /// index so future inserts can land.
    fn take_contiguous_from(&mut self, expected: &mut u64) -> Vec<Bytes> {
        let mut ready = Vec::new();
        let mut chunks_to_advance = 0u64;

        // First pass: collect all contiguous chunks
        loop {
            if *expected < self.base_index {
                break;
            }
            let offset = *expected - self.base_index;
            if offset >= self.slots.len() as u64 {
                break;
            }
            let idx = self.slot_index(offset);
            match self.slots[idx].take() {
                Some(bytes) => {
                    ready.push(bytes);
                    *expected += 1;
                    chunks_to_advance += 1;
                }
                None => break,
            }
        }

        // Bulk advance the window (if we collected any chunks)
        if chunks_to_advance > 0 {
            self.advance_window_by(chunks_to_advance);
        }

        ready
    }

    /// Translate a logical offset relative to `base_index` into a circular slot.
    fn slot_index(&self, offset: u64) -> usize {
        if self.slots.is_empty() {
            return 0;
        }
        (self.head + offset as usize) % self.slots.len()
    }

    /// Advance the window by the specified number of slots.
    /// This efficiently handles both single and bulk advances.
    fn advance_window_by(&mut self, count: u64) {
        if count == 0 {
            return;
        }

        self.base_index = self.base_index.saturating_add(count);
        if !self.slots.is_empty() {
            // For large advances, use modulo to avoid overflow
            let count_usize = count as usize;
            self.head = (self.head + count_usize) % self.slots.len();
        }
    }
}

/// Borrowed state required to evaluate a DATA frame.
struct FrameCtx<'a> {
    data: &'a lossless_session::LosslessSessionData,
    payload: Bytes,
    expected: &'a mut u64,
    pending: &'a mut PendingWindow,
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

    if pending.insert(idx, payload) {
        trace!(
            chunk_index = idx,
            "Lossless receiver: chunk stored for ordering"
        );
    }

    let ready_chunks = pending.take_contiguous_from(expected);
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
    fn pending_window_bulk_advance() {
        let mut window = PendingWindow::new(16, 1);
        let mut expected = 1u64;

        // Insert chunks 1-5 in order
        for i in 1..=5 {
            let payload = Bytes::from(vec![i as u8; 100]);
            assert!(window.insert(i, payload), "Should insert chunk {}", i);
        }

        // Drain all contiguous chunks (should advance by 5)
        let ready = window.take_contiguous_from(&mut expected);
        assert_eq!(ready.len(), 5, "Should have drained 5 chunks");
        assert_eq!(expected, 6, "Expected should advance to 6");
        assert_eq!(window.base_index, 6, "Base index should advance to 6");
        assert_eq!(window.head, 5, "Head should advance by 5");

        // Insert chunk 10 (out of order)
        let payload = Bytes::from(vec![10u8; 100]);
        assert!(window.insert(10, payload), "Should insert chunk 10");

        // Try to drain - should get nothing since 6-9 are missing
        let ready = window.take_contiguous_from(&mut expected);
        assert_eq!(ready.len(), 0, "Should not drain non-contiguous chunks");
        assert_eq!(expected, 6, "Expected should stay at 6");
        assert_eq!(window.base_index, 6, "Base index should stay at 6");

        // Fill in chunks 6-9
        for i in 6..=9 {
            let payload = Bytes::from(vec![i as u8; 100]);
            assert!(window.insert(i, payload), "Should insert chunk {}", i);
        }

        // Now drain should get 6-10 (5 chunks) in one bulk operation
        let ready = window.take_contiguous_from(&mut expected);
        assert_eq!(ready.len(), 5, "Should drain chunks 6-10");
        assert_eq!(expected, 11, "Expected should advance to 11");
        assert_eq!(window.base_index, 11, "Base index should advance to 11");
        // Head advanced by 5 from position 5: (5 + 5) % 16 = 10
        assert_eq!(window.head, 10, "Head should wrap correctly");
    }

    #[test]
    fn pending_window_wrapping() {
        let mut window = PendingWindow::new(8, 1);
        let mut expected = 1u64;

        // Insert and drain enough to wrap around
        for batch in 0..3 {
            let start = batch * 8 + 1;
            for i in start..start + 8 {
                let payload = Bytes::from(vec![i as u8; 100]);
                assert!(window.insert(i, payload));
            }
            let ready = window.take_contiguous_from(&mut expected);
            assert_eq!(ready.len(), 8);
            assert_eq!(expected, start + 8);
        }

        // After 3 batches of 8, we should have advanced 24 slots
        assert_eq!(window.base_index, 25);
        // Head should wrap: (0 + 24) % 8 = 0
        assert_eq!(window.head, 0);
    }

    #[test]
    fn pending_window_duplicate_insert() {
        let mut window = PendingWindow::new(16, 1);

        let payload1 = Bytes::from(vec![1u8; 100]);
        let payload2 = Bytes::from(vec![2u8; 100]);

        // First insert should succeed
        assert!(window.insert(5, payload1), "First insert should succeed");

        // Duplicate insert should fail
        assert!(!window.insert(5, payload2), "Duplicate insert should fail");
    }

    #[test]
    fn pending_window_out_of_range() {
        let mut window = PendingWindow::new(8, 10);

        // Below base_index
        let payload = Bytes::from(vec![1u8; 100]);
        assert!(
            !window.insert(5, payload),
            "Should reject chunk below base_index"
        );

        // Beyond window size
        let payload = Bytes::from(vec![2u8; 100]);
        assert!(
            !window.insert(20, payload),
            "Should reject chunk beyond window"
        );
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
}
