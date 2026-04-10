//! Online peel decoder for the METTLE core.

use std::collections::{BTreeMap, HashMap, VecDeque};

use smallvec::SmallVec;

use super::encoder::{MettleBin, xor_into};
use super::hash::source_signature;
use super::params::{MettleParams, PAPER_EDGE_COUNT};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DecodedSource {
    pub source_id: u64,
    pub payload: Vec<u8>,
    pub decoded_at_bin: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct DecoderStats {
    pub received_bins: usize,
    pub decoded_sources: usize,
}

#[derive(Debug, Clone)]
struct BinState {
    degree: u32,
    xor_source_id: u64,
    xor_source_sig: u64,
    payload: Vec<u8>,
}

#[derive(Debug)]
pub struct Decoder {
    params: MettleParams,
    total_sources: u64,
    received_bins: BTreeMap<u64, BinState>,
    decoded_sources: Vec<Option<DecodedSource>>,
    pending_peels: HashMap<u64, SmallVec<[u64; PAPER_EDGE_COUNT]>>,
    singleton_queue: VecDeque<u64>,
    seen_bins: usize,
}

impl Decoder {
    #[must_use]
    pub fn new(params: MettleParams, total_sources: u64) -> Self {
        Self {
            params,
            total_sources,
            received_bins: BTreeMap::new(),
            decoded_sources: vec![None; total_sources as usize],
            pending_peels: HashMap::new(),
            singleton_queue: VecDeque::new(),
            seen_bins: 0,
        }
    }

    pub fn receive_bin(&mut self, bin: MettleBin) -> Vec<DecodedSource> {
        if self.received_bins.contains_key(&bin.bin_id) {
            return Vec::new();
        }
        self.seen_bins += 1;

        let bin_id = bin.bin_id;
        let mut state = BinState {
            degree: bin.degree,
            xor_source_id: bin.xor_source_id,
            xor_source_sig: bin.xor_source_sig,
            payload: bin.payload,
        };
        self.apply_pending_peels(bin_id, &mut state);
        if state.degree == 1 {
            self.singleton_queue.push_back(bin_id);
        }
        self.received_bins.insert(bin_id, state);
        self.run_peeling(bin_id)
    }

    #[must_use]
    pub fn stats(&self) -> DecoderStats {
        DecoderStats {
            received_bins: self.seen_bins,
            decoded_sources: self
                .decoded_sources
                .iter()
                .filter(|decoded| decoded.is_some())
                .count(),
        }
    }

    #[must_use]
    pub fn is_complete(&self) -> bool {
        self.decoded_sources.iter().all(|decoded| decoded.is_some())
    }

    #[must_use]
    pub fn rebuild_object(&self, total_bytes: usize) -> Option<Vec<u8>> {
        if !self.is_complete() {
            return None;
        }
        let mut object =
            Vec::with_capacity(self.total_sources as usize * self.params.source_symbol_bytes);
        for decoded in self.decoded_sources.iter().flatten() {
            object.extend_from_slice(&decoded.payload);
        }
        object.truncate(total_bytes);
        Some(object)
    }

    #[must_use]
    pub fn first_undecoded_source_id(&self) -> Option<u64> {
        self.decoded_sources
            .iter()
            .position(|decoded| decoded.is_none())
            .map(|idx| idx as u64)
    }

    #[must_use]
    pub fn decoded_prefix_len(&self) -> u64 {
        self.decoded_sources
            .iter()
            .take_while(|decoded| decoded.is_some())
            .count() as u64
    }

    fn apply_pending_peels(&mut self, bin_id: u64, bin: &mut BinState) {
        let Some(source_ids) = self.pending_peels.remove(&bin_id) else {
            return;
        };
        for source_id in source_ids {
            let Some(decoded) = self.decoded_sources[source_id as usize].as_ref() else {
                continue;
            };
            peel_one(
                bin,
                source_id,
                source_signature(self.params.seed, source_id),
                &decoded.payload,
            );
        }
    }

    fn run_peeling(&mut self, current_arrival_bin_id: u64) -> Vec<DecodedSource> {
        let mut newly_decoded = Vec::new();
        while let Some(bin_id) = self.singleton_queue.pop_front() {
            let Some(bin) = self.received_bins.get(&bin_id).cloned() else {
                continue;
            };
            if bin.degree != 1 {
                continue;
            }
            let source_id = bin.xor_source_id;
            if source_id >= self.total_sources {
                continue;
            }
            let expected_sig = source_signature(self.params.seed, source_id);
            if bin.xor_source_sig != expected_sig {
                continue;
            }
            if self.decoded_sources[source_id as usize].is_some() {
                continue;
            }

            let decoded = DecodedSource {
                source_id,
                payload: bin.payload.clone(),
                // Decoding happens when the current arrival makes the peel cascade possible,
                // not necessarily at the singleton bin's original position.
                decoded_at_bin: current_arrival_bin_id,
            };
            self.decoded_sources[source_id as usize] = Some(decoded.clone());
            newly_decoded.push(decoded.clone());

            for neighbor_bin_id in self.params.edges_for(source_id) {
                if let Some(neighbor) = self.received_bins.get_mut(&neighbor_bin_id) {
                    peel_one(neighbor, source_id, expected_sig, &decoded.payload);
                    if neighbor.degree == 1 {
                        self.singleton_queue.push_back(neighbor_bin_id);
                    }
                } else {
                    self.pending_peels
                        .entry(neighbor_bin_id)
                        .or_default()
                        .push(source_id);
                }
            }
        }
        newly_decoded
    }
}

fn peel_one(bin: &mut BinState, source_id: u64, source_sig: u64, payload: &[u8]) {
    if bin.degree == 0 {
        return;
    }
    bin.degree -= 1;
    bin.xor_source_id ^= source_id;
    bin.xor_source_sig ^= source_sig;
    xor_into(&mut bin.payload, payload);
}


