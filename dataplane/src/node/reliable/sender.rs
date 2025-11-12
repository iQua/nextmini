use bytes::Bytes;
use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::fs::File;
use std::io::{self, Read};
use std::net::Ipv4Addr;
use std::time::{Duration, Instant};

use tokio::sync::mpsc;

use nextmini_messages::rlm::{self, RlmControl};

use crate::node::packet::Packet;
use crate::node::processor::ProcessorHandle;
use crate::node::{NodeId, NodeIdExt};

use super::api::InboundFrame;
use super::control::{self, CompletionPolicy};
use super::session::{AckPolicy, CommonConfig, SenderConfig};

const DEFAULT_WINDOW: usize = 64;

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
        (total_bytes + chunk_size - 1) / chunk_size
    };

    tracing::info!(
        session_id = sid,
        total_bytes,
        total_chunks,
        receivers = cfg.receiver_ids.len(),
        "RLM sender started"
    );

    let source_file = cfg
        .source_path
        .as_ref()
        .and_then(|path| match File::open(path) {
            Ok(file) => Some(file),
            Err(error) => {
                tracing::error!(
                    session_id = sid,
                    path = %path,
                    %error,
                    "RLM sender: unable to open source file; falling back to empty chunks"
                );
                None
            }
        });

    let completion_policy = completion_from_ack(&cfg.ack_policy, cfg.receiver_ids.len());
    let mut state = SenderState::new(cfg, total_chunks, completion_policy);
    let mut chunk_source =
        ChunkSource::new(source_file, state.common.chunk_size, total_chunks, sid);
    let mut pacer = DataPacer::new(state.common.data_bucket.clone());
    let mut last_resend = Instant::now();

    state.send_manifest(&processors);

    loop {
        while let Ok(frame) = ctrl_rx.try_recv() {
            state.handle_control(frame);
        }

        state.maybe_release_ready_gate();

        let mut progressed = false;

        if state.ready_for_data() && !chunk_source.finished() && state.inflight_len() < state.window
        {
            match chunk_source.next_chunk() {
                Ok(Some(chunk)) => {
                    pacer.wait_for(state.common.chunk_size).await;
                    state.send_data_chunk(chunk, &processors);
                    progressed = true;
                }
                Ok(None) => {
                    state.mark_source_drained();
                    progressed = true;
                }
                Err(error) => {
                    tracing::error!(
                        session_id = sid,
                        %error,
                        "RLM sender: read error; stopping new chunk generation"
                    );
                    state.mark_source_drained();
                    progressed = true;
                }
            }
        }

        if !progressed
            && state.ready_for_data()
            && state.should_resend()
            && last_resend.elapsed() >= state.repair_backoff
        {
            if state.send_resend(&processors) {
                last_resend = Instant::now();
                progressed = true;
            }
        }

        if !progressed && state.try_emit_eot(&processors) {
            progressed = true;
        }

        if state.is_complete() {
            tracing::info!(
                session_id = sid,
                bytes_sent = state.bytes_sent,
                chunks_sent = state.primary_chunks,
                resends = state.resend_count,
                "RLM sender finished with reliable delivery guarantees"
            );
            break;
        }

        if !progressed {
            // Don't block forever; let the loop re-check timers (e.g., MANIFEST resend).
            if let Ok(Some(frame)) =
                tokio::time::timeout(Duration::from_millis(20), ctrl_rx.recv()).await
            {
                state.handle_control(frame);
            }
            // On timeout or channel closed, just fall through and loop; this allows
            // MANIFEST re-sends every 250ms while waiting for READY.
        }
    }
}

fn completion_from_ack(policy: &AckPolicy, receiver_count: usize) -> CompletionPolicy {
    match policy {
        AckPolicy::All => CompletionPolicy::All,
        AckPolicy::KofN(k) => CompletionPolicy::Threshold((*k).max(1).min(receiver_count.max(1))),
        AckPolicy::Fraction(fraction) => {
            let needed = ((*fraction * receiver_count as f32).ceil() as usize).max(1);
            CompletionPolicy::Threshold(needed.min(receiver_count.max(1)))
        }
    }
}

struct SenderState {
    session_id: u64,
    common: CommonConfig,
    completion_policy: CompletionPolicy,
    receiver_count: usize,
    window: usize,
    total_chunks: u64,
    total_bytes: u64,
    inflight: BTreeMap<u64, HashSet<usize>>,
    frame_cache: BTreeMap<u64, Bytes>,
    resend_queue: BTreeSet<u64>,
    ready_nodes: HashSet<usize>,
    ready_gate_open: bool,
    ready_deadline: Option<Instant>,
    source_drained: bool,
    eot_sent: bool,
    bytes_sent: u64,
    primary_chunks: u64,
    resend_count: u64,
    src_ip: Ipv4Addr,
    dst_ip: Ipv4Addr,
    src_port: u16,
    dst_port: u16,
    repair_backoff: Duration,
}

impl SenderState {
    fn new(cfg: SenderConfig, total_chunks: u64, completion_policy: CompletionPolicy) -> Self {
        let common = cfg.common.clone();
        let receiver_count = cfg.receiver_ids.len();
        let ready_gate_open = receiver_count == 0;
        let ready_grace_ms = cfg.ready_grace_ms.max(1);
        let ready_deadline = if ready_gate_open {
            None
        } else {
            Some(Instant::now() + Duration::from_millis(ready_grace_ms))
        };
        let src_ip = (common.local_node_id as NodeId)
            .ip_addr(common.user_space_base_addr, common.local_netmask);
        let dst_ip = common.group_ip;
        let window = compute_window(&cfg);

        if cfg.common.control_weight != 0 {
            tracing::debug!(
                session_id = common.session_id,
                control_weight = cfg.common.control_weight,
                "RLM sender: control_weight is recorded but scheduler boosts are not yet wired."
            );
        }
        if cfg.checksum_out {
            tracing::warn!(
                session_id = common.session_id,
                "RLM sender: checksum_out requested but checksum emission is not implemented; skipping."
            );
        }
        if cfg.fec_k.is_some() || cfg.fec_p != 0 {
            tracing::warn!(
                session_id = common.session_id,
                fec_k = ?cfg.fec_k,
                fec_p = cfg.fec_p,
                "RLM sender: FEC parameters supplied but FEC is not yet implemented."
            );
        }

        Self {
            session_id: common.session_id,
            common,
            completion_policy,
            receiver_count,
            window,
            total_chunks,
            total_bytes: cfg.total_bytes,
            inflight: BTreeMap::new(),
            frame_cache: BTreeMap::new(),
            resend_queue: BTreeSet::new(),
            ready_nodes: HashSet::new(),
            ready_gate_open,
            ready_deadline,
            source_drained: total_chunks == 0,
            eot_sent: false,
            bytes_sent: 0,
            primary_chunks: 0,
            resend_count: 0,
            src_ip,
            dst_ip,
            src_port: cfg.common.src_port,
            dst_port: cfg.common.dst_port,
            repair_backoff: Duration::from_millis(cfg.repair_backoff_ms.max(1)),
        }
    }

    fn send_manifest(&self, processors: &ProcessorHandle) {
        let manifest = RlmControl::Manifest {
            chunk_size: self.common.chunk_size as u32,
            total_bytes: self.total_bytes,
            checksum_algo: 0,
            options: 0,
        };
        self.send_control(&manifest, processors);
        tracing::info!(session_id = self.session_id, "RLM sender: MANIFEST sent");
    }

    fn ready_for_data(&self) -> bool {
        self.ready_gate_open && !self.source_drained
    }

    fn inflight_len(&self) -> usize {
        self.inflight.len()
    }

    fn should_resend(&self) -> bool {
        !self.resend_queue.is_empty()
    }

    fn mark_source_drained(&mut self) {
        self.source_drained = true;
    }

    fn send_data_chunk(&mut self, chunk: ChunkPayload, processors: &ProcessorHandle) {
        let frame = rlm::encode_data(self.session_id, chunk.index, &chunk.data);
        let bytes = Bytes::from(frame);
        self.enqueue_frame(chunk.index, bytes.clone());
        self.bytes_sent += chunk.data.len() as u64;
        self.primary_chunks += 1;
        self.send_frame(&bytes, processors);
    }

    fn send_resend(&mut self, processors: &ProcessorHandle) -> bool {
        let Some(idx) = self.resend_queue.iter().next().copied() else {
            return false;
        };
        if let Some(frame) = self.frame_cache.get(&idx).cloned() {
            self.resend_queue.remove(&idx);
            self.resend_count += 1;
            self.send_frame(&frame, processors);
            tracing::debug!(
                session_id = self.session_id,
                index = idx,
                "RLM sender: retransmit"
            );
            true
        } else {
            self.resend_queue.remove(&idx);
            false
        }
    }

    fn try_emit_eot(&mut self, processors: &ProcessorHandle) -> bool {
        if self.eot_sent || !self.source_drained || !self.inflight.is_empty() {
            return false;
        }
        let eot = RlmControl::Eot {
            last_index: self.total_chunks,
            checksum: None,
        };
        self.send_control(&eot, processors);
        self.eot_sent = true;
        tracing::info!(
            session_id = self.session_id,
            last_index = self.total_chunks,
            "RLM sender: EOT sent"
        );
        true
    }

    fn is_complete(&self) -> bool {
        self.source_drained
            && self.eot_sent
            && self.inflight.is_empty()
            && self.resend_queue.is_empty()
    }

    fn handle_control(&mut self, frame: InboundFrame) {
        let InboundFrame { bytes, peer_id, .. } = frame;
        let Some((_, control)) = rlm::decode_control(&bytes) else {
            tracing::warn!(
                session_id = self.session_id,
                "RLM sender: failed to decode control frame"
            );
            return;
        };

        match control {
            RlmControl::Ready { node_id } => {
                self.ready_nodes.insert(node_id as usize);
                tracing::debug!(
                    session_id = self.session_id,
                    node_id,
                    "RLM sender: receiver ready"
                );
            }
            RlmControl::Manifest { .. } | RlmControl::Eot { .. } => {
                // Ignore sender-originated control frames looped back.
            }
            _ => {
                let Some(from_node) = peer_id else {
                    tracing::warn!(
                        session_id = self.session_id,
                        ?control,
                        "RLM sender: dropping control without peer id"
                    );
                    return;
                };
                let retired = control::process_control_event(
                    from_node,
                    &control,
                    &mut self.inflight,
                    &mut self.resend_queue,
                    self.receiver_count.max(1),
                    &self.completion_policy,
                );
                if !retired.is_empty() {
                    control::retire_chunks(&retired, &mut self.inflight, &mut self.resend_queue);
                    for idx in retired {
                        self.frame_cache.remove(&idx);
                    }
                }
            }
        }
    }

    fn enqueue_frame(&mut self, idx: u64, frame: Bytes) {
        self.frame_cache.insert(idx, frame);
        self.inflight.entry(idx).or_insert_with(HashSet::new);
    }

    fn send_frame(&self, frame: &Bytes, processors: &ProcessorHandle) {
        let packet = Packet::build_ipv4_tcp_packet(
            self.src_ip,
            self.src_port,
            self.dst_ip,
            self.dst_port,
            frame,
        );
        processors.process_packet(packet);
    }

    fn send_control(&self, control: &RlmControl, processors: &ProcessorHandle) {
        let buf = rlm::encode_control(self.session_id, control);
        let packet = Packet::build_ipv4_tcp_packet(
            self.src_ip,
            self.src_port,
            self.dst_ip,
            self.dst_port,
            &buf,
        );
        processors.process_packet(packet);
    }

    fn maybe_release_ready_gate(&mut self) {
        if self.ready_gate_open {
            return;
        }
        if self.receiver_count > 0 && self.ready_nodes.len() == self.receiver_count {
            self.ready_gate_open = true;
            self.ready_deadline = None;
            tracing::info!(
                session_id = self.session_id,
                "RLM sender: all receivers ready"
            );
            return;
        }
        if let Some(deadline) = self.ready_deadline {
            if Instant::now() >= deadline {
                self.ready_gate_open = true;
                self.ready_deadline = None;
                tracing::warn!(
                    session_id = self.session_id,
                    ready = self.ready_nodes.len(),
                    total = self.receiver_count,
                    "RLM sender: proceeding without all receivers ready"
                );
            }
        }
    }
}

fn compute_window(cfg: &SenderConfig) -> usize {
    let receiver_factor = (cfg.receiver_ids.len().max(1)) * 2;
    let mut window = DEFAULT_WINDOW.max(receiver_factor);
    if let Some(bucket) = &cfg.common.data_bucket {
        if cfg.common.chunk_size > 0 {
            let per_chunk = cfg.common.chunk_size;
            let bucket_chunks = (bucket.bucket_size / per_chunk).max(1);
            window = window.max(bucket_chunks);
        }
    }
    window
}

struct ChunkPayload {
    index: u64,
    data: Vec<u8>,
}

struct ChunkSource {
    file: Option<File>,
    chunk_size: usize,
    total_chunks: u64,
    next_index: u64,
    session_id: u64,
}

impl ChunkSource {
    fn new(file: Option<File>, chunk_size: usize, total_chunks: u64, session_id: u64) -> Self {
        Self {
            file,
            chunk_size,
            total_chunks,
            next_index: 1,
            session_id,
        }
    }

    fn finished(&self) -> bool {
        self.total_chunks == 0 || self.next_index > self.total_chunks
    }

    fn next_chunk(&mut self) -> io::Result<Option<ChunkPayload>> {
        if self.finished() {
            return Ok(None);
        }

        let idx = self.next_index;

        if let Some(file) = self.file.as_mut() {
            let mut buf = vec![0u8; self.chunk_size];
            let read = file.read(&mut buf)?;
            if read == 0 {
                tracing::warn!(
                    session_id = self.session_id,
                    "RLM sender: source file ended unexpectedly"
                );
                self.next_index = self.total_chunks + 1;
                return Ok(None);
            }
            buf.truncate(read);
            self.next_index += 1;
            Ok(Some(ChunkPayload {
                index: idx,
                data: buf,
            }))
        } else {
            self.next_index += 1;
            Ok(Some(ChunkPayload {
                index: idx,
                data: Vec::new(),
            }))
        }
    }
}

struct DataPacer {
    spec: Option<nextmini_messages::TokenBucketSpec>,
    tokens: f64,
    last: Instant,
}

impl DataPacer {
    fn new(spec: Option<nextmini_messages::TokenBucketSpec>) -> Self {
        let tokens = spec.as_ref().map(|tb| tb.bucket_size as f64).unwrap_or(0.0);
        Self {
            spec,
            tokens,
            last: Instant::now(),
        }
    }

    async fn wait_for(&mut self, bytes: usize) {
        let Some(spec) = self.spec.clone() else {
            return;
        };
        let bytes_f = bytes as f64;
        loop {
            self.refill(&spec);
            if self.tokens >= bytes_f {
                self.tokens -= bytes_f;
                break;
            }
            let needed = (bytes_f - self.tokens).max(1.0);
            let wait = needed / spec.rate.max(1) as f64;
            tokio::time::sleep(Duration::from_secs_f64(wait.max(0.001))).await;
        }
    }

    fn refill(&mut self, spec: &nextmini_messages::TokenBucketSpec) {
        let now = Instant::now();
        let elapsed = now.duration_since(self.last).as_secs_f64();
        self.tokens = (self.tokens + elapsed * spec.rate as f64).min(spec.bucket_size as f64);
        self.last = now;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn data_roundtrip_header_and_meta() {
        let sid = 99;
        let idx = 7;
        let plen = 4096usize;
        let payload = vec![0xAAu8; plen];
        let buf = rlm::encode_data(sid, idx, &payload);
        let (hdr, data, body) = rlm::decode_data(&buf).expect("decode data");
        assert_eq!(hdr.session_id, sid);
        assert_eq!(data.index, idx);
        assert_eq!(data.payload_len as usize, plen);
        assert_eq!(body.len(), plen);
    }
}
