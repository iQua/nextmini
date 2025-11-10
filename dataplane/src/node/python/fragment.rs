use std::time::{Duration, Instant};

use ahash::AHashMap;

use crate::node::FlowId;
use crate::node::packet::PyPayloadSegHeader;

#[derive(Debug, Clone)]
pub struct FragmentAssemblerConfig {
    pub enabled: bool,
    pub max_message_bytes: usize,
    pub reassembly_window_bytes: usize,
    pub fragment_timeout: Duration,
}

#[derive(Debug, Clone)]
pub struct InsertReport {
    pub evicted: Vec<EvictedMessage>,
    pub result: FragmentResult,
}

impl InsertReport {
    fn bypassed() -> Self {
        Self {
            evicted: Vec::new(),
            result: FragmentResult::Bypassed,
        }
    }
}

#[derive(Debug, Clone)]
pub enum FragmentResult {
    Pending,
    Complete(ReassembledMessage),
    Dropped(FragmentDrop),
    Bypassed,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FragmentDropKind {
    WindowOverflow,
    AssemblerDrop,
}

#[derive(Debug, Clone)]
pub struct FragmentDrop {
    pub kind: FragmentDropKind,
    pub detail: String,
}

impl FragmentDrop {
    fn assembler(detail: impl Into<String>) -> Self {
        Self {
            kind: FragmentDropKind::AssemblerDrop,
            detail: detail.into(),
        }
    }

    fn window_overflow(detail: impl Into<String>) -> Self {
        Self {
            kind: FragmentDropKind::WindowOverflow,
            detail: detail.into(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReassembledMessage {
    pub flow_id: FlowId,
    pub message_id: u64,
    pub payload: Vec<u8>,
    pub header_prefix: Option<Vec<u8>>,
    pub total_len: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EvictedMessage {
    pub flow_id: FlowId,
    pub message_id: u64,
    pub missing_fragments: usize,
}

#[derive(Debug)]
pub struct FragmentAssembler {
    cfg: FragmentAssemblerConfig,
    inflight: AHashMap<(FlowId, u64), ReassemblyEntry>,
    inflight_bytes: usize,
}

impl FragmentAssembler {
    pub fn new(cfg: FragmentAssemblerConfig) -> Self {
        Self {
            cfg,
            inflight: AHashMap::new(),
            inflight_bytes: 0,
        }
    }

    pub fn insert_fragment(
        &mut self,
        flow_id: FlowId,
        header: PyPayloadSegHeader,
        chunk: Vec<u8>,
        header_prefix: Option<Vec<u8>>,
        now: Instant,
    ) -> InsertReport {
        if !self.cfg.enabled {
            return InsertReport::bypassed();
        }

        let mut report = InsertReport {
            evicted: self.evict_expired(now),
            result: FragmentResult::Pending,
        };

        if header.total_len as usize > self.cfg.max_message_bytes {
            report.result = FragmentResult::Dropped(FragmentDrop::assembler(format!(
                "message {} on flow {} exceeds limit {} bytes ({}).",
                header.message_id, flow_id, self.cfg.max_message_bytes, header.total_len
            )));
            return report;
        }

        if header.fragment_count == 0 {
            report.result =
                FragmentResult::Dropped(FragmentDrop::assembler("fragment_count is zero."));
            return report;
        }

        if header.fragment_payload_len as usize != chunk.len() {
            report.result = FragmentResult::Dropped(FragmentDrop::assembler(format!(
                "fragment payload len mismatch (header={}, actual={}).",
                header.fragment_payload_len,
                chunk.len()
            )));
            return report;
        }

        if self.cfg.reassembly_window_bytes > 0
            && self.inflight_bytes + chunk.len() > self.cfg.reassembly_window_bytes
        {
            report.result = FragmentResult::Dropped(FragmentDrop::window_overflow(format!(
                "fragment window {} bytes exceeded for flow {} message {}.",
                self.cfg.reassembly_window_bytes, flow_id, header.message_id
            )));
            return report;
        }

        let key = (flow_id, header.message_id);
        let entry = self.inflight.entry(key).or_insert_with(|| {
            ReassemblyEntry::new(
                flow_id,
                header.message_id,
                header.total_len as usize,
                header.fragment_count as usize,
                now + self.cfg.fragment_timeout,
            )
        });

        match entry.insert_fragment(
            &header,
            chunk,
            header_prefix,
            now + self.cfg.fragment_timeout,
        ) {
            Ok(EntryInsertOutcome::Duplicate) => {
                report.result = FragmentResult::Pending;
            }
            Ok(EntryInsertOutcome::Pending { bytes_added }) => {
                self.inflight_bytes += bytes_added;
                report.result = FragmentResult::Pending;
            }
            Ok(EntryInsertOutcome::Complete {
                bytes_added,
                payload,
                header_prefix,
            }) => {
                self.inflight_bytes += bytes_added;
                self.inflight.remove(&key);
                report.result = FragmentResult::Complete(ReassembledMessage {
                    flow_id,
                    message_id: header.message_id,
                    payload,
                    header_prefix,
                    total_len: header.total_len as usize,
                });
            }
            Err(detail) => {
                self.inflight.remove(&key);
                report.result = FragmentResult::Dropped(FragmentDrop::assembler(detail));
            }
        }

        report
    }

    fn evict_expired(&mut self, now: Instant) -> Vec<EvictedMessage> {
        let expired: Vec<_> = self
            .inflight
            .iter()
            .filter_map(|(key, entry)| {
                if entry.deadline <= now {
                    Some(*key)
                } else {
                    None
                }
            })
            .collect();
        let mut evicted = Vec::new();
        for key in expired {
            if let Some(entry) = self.inflight.remove(&key) {
                self.inflight_bytes = self.inflight_bytes.saturating_sub(entry.buffered_bytes);
                evicted.push(EvictedMessage {
                    flow_id: entry.flow_id,
                    message_id: entry.message_id,
                    missing_fragments: entry.missing_fragments(),
                });
            }
        }
        evicted
    }
}

#[derive(Debug)]
struct ReassemblyEntry {
    flow_id: FlowId,
    message_id: u64,
    total_len: usize,
    fragment_count: usize,
    buffered_bytes: usize,
    fragments: Vec<Option<Vec<u8>>>,
    header_prefix: Option<Vec<u8>>,
    deadline: Instant,
}

impl ReassemblyEntry {
    fn new(
        flow_id: FlowId,
        message_id: u64,
        total_len: usize,
        fragment_count: usize,
        deadline: Instant,
    ) -> Self {
        let count = fragment_count.max(1);
        Self {
            flow_id,
            message_id,
            total_len,
            fragment_count: count,
            buffered_bytes: 0,
            fragments: vec![None; count],
            header_prefix: None,
            deadline,
        }
    }

    fn insert_fragment(
        &mut self,
        header: &PyPayloadSegHeader,
        chunk: Vec<u8>,
        header_prefix: Option<Vec<u8>>,
        new_deadline: Instant,
    ) -> Result<EntryInsertOutcome, String> {
        if header.fragment_count as usize != self.fragment_count {
            return Err(format!(
                "fragment_count mismatch (saw {}, expected {}).",
                header.fragment_count, self.fragment_count
            ));
        }
        if header.total_len as usize != self.total_len {
            return Err(format!(
                "total_len mismatch (saw {}, expected {}).",
                header.total_len, self.total_len
            ));
        }
        let idx = header.fragment_index as usize;
        if idx >= self.fragment_count {
            return Err(format!(
                "fragment_index {} out of range (max {}).",
                idx,
                self.fragment_count - 1
            ));
        }

        if self.fragments[idx].is_some() {
            return Ok(EntryInsertOutcome::Duplicate);
        }

        if idx == 0 {
            if let Some(prefix) = header_prefix {
                self.header_prefix = Some(prefix);
            }
        }

        let chunk_len = chunk.len();
        self.fragments[idx] = Some(chunk);
        self.buffered_bytes += chunk_len;
        self.deadline = new_deadline;

        if self.fragments.iter().all(|frag| frag.is_some()) {
            let mut payload = Vec::with_capacity(self.total_len);
            for fragment in self.fragments.iter_mut() {
                let bytes = fragment
                    .take()
                    .ok_or_else(|| "fragment missing during assembly.".to_string())?;
                payload.extend_from_slice(&bytes);
            }
            if payload.len() != self.total_len {
                return Err(format!(
                    "assembled payload len {} mismatches expected {}.",
                    payload.len(),
                    self.total_len
                ));
            }
            Ok(EntryInsertOutcome::Complete {
                bytes_added: chunk_len,
                payload,
                header_prefix: self.header_prefix.clone(),
            })
        } else {
            Ok(EntryInsertOutcome::Pending {
                bytes_added: chunk_len,
            })
        }
    }

    fn missing_fragments(&self) -> usize {
        self.fragments.iter().filter(|frag| frag.is_none()).count()
    }
}

enum EntryInsertOutcome {
    Duplicate,
    Pending {
        bytes_added: usize,
    },
    Complete {
        bytes_added: usize,
        payload: Vec<u8>,
        header_prefix: Option<Vec<u8>>,
    },
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::node::packet::PyPayloadSegHeader;

    fn cfg() -> FragmentAssemblerConfig {
        FragmentAssemblerConfig {
            enabled: true,
            max_message_bytes: 64 * 1024,
            reassembly_window_bytes: 64 * 1024,
            fragment_timeout: Duration::from_secs(1),
        }
    }

    fn header(mid: u64, idx: u16, count: u16, total: usize, len: usize) -> PyPayloadSegHeader {
        PyPayloadSegHeader {
            fragmented: count > 1,
            last_fragment: count > 1 && idx + 1 == count,
            message_id: mid,
            total_len: total as u32,
            fragment_index: idx,
            fragment_count: count,
            fragment_payload_len: len as u32,
        }
    }

    #[test]
    fn single_fragment_completes() {
        let mut assembler = FragmentAssembler::new(cfg());
        let now = Instant::now();
        let payload = vec![0xAA; 128];
        let report = assembler.insert_fragment(
            5,
            header(1, 0, 1, payload.len(), payload.len()),
            payload.clone(),
            Some(vec![0u8; 40]),
            now,
        );
        assert!(report.evicted.is_empty());
        match report.result {
            FragmentResult::Complete(msg) => {
                assert_eq!(msg.payload, payload);
                assert!(msg.header_prefix.is_some());
            }
            other => panic!("unexpected result: {:?}", other),
        }
    }

    #[test]
    fn multi_fragment_requires_all_chunks() {
        let mut assembler = FragmentAssembler::new(cfg());
        let now = Instant::now();
        let chunk = vec![1u8; 64];

        let pending = assembler.insert_fragment(
            9,
            header(7, 0, 2, 128, chunk.len()),
            chunk.clone(),
            Some(vec![0u8; 40]),
            now,
        );
        assert!(matches!(pending.result, FragmentResult::Pending));

        let completed = assembler.insert_fragment(
            9,
            header(7, 1, 2, 128, chunk.len()),
            chunk.clone(),
            None,
            now,
        );
        match completed.result {
            FragmentResult::Complete(msg) => assert_eq!(msg.payload.len(), 128),
            other => panic!("expected completion, got {:?}", other),
        }
    }

    #[test]
    fn window_overflow_drops() {
        let mut cfg = cfg();
        cfg.reassembly_window_bytes = 32;
        let mut assembler = FragmentAssembler::new(cfg);
        let now = Instant::now();
        let report = assembler.insert_fragment(
            3,
            header(5, 0, 2, 64, 48),
            vec![0u8; 48],
            Some(vec![0u8; 40]),
            now,
        );
        match report.result {
            FragmentResult::Dropped(drop) => {
                assert_eq!(drop.kind, FragmentDropKind::WindowOverflow);
            }
            other => panic!("expected drop, got {:?}", other),
        }
    }

    #[test]
    fn evicts_on_timeout() {
        let mut assembler = FragmentAssembler::new(cfg());
        let now = Instant::now();
        let _ = assembler.insert_fragment(
            1,
            header(99, 0, 2, 64, 32),
            vec![0u8; 32],
            Some(vec![0u8; 40]),
            now,
        );
        let evicted = assembler.evict_expired(now + Duration::from_secs(2));
        assert_eq!(evicted.len(), 1);
        assert_eq!(evicted[0].message_id, 99);
    }
}
