//! Nominal METTLE encoder.

use std::collections::BTreeMap;

use super::hash::source_signature;
use super::params::MettleParams;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MettleBin {
    pub bin_id: u64,
    pub degree: u32,
    pub xor_source_id: u64,
    pub xor_source_sig: u64,
    pub payload: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EncodedStream {
    pub params: MettleParams,
    pub total_bytes: usize,
    pub total_sources: u64,
    pub final_bin_exclusive: u64,
    pub bins: Vec<MettleBin>,
}

#[derive(Debug)]
pub struct Encoder {
    params: MettleParams,
    next_source_id: u64,
    total_bytes: usize,
    emitted_bins: Vec<MettleBin>,
    open_bins: BTreeMap<u64, BinAccumulator>,
}

#[derive(Debug, Clone)]
struct BinAccumulator {
    degree: u32,
    xor_source_id: u64,
    xor_source_sig: u64,
    payload: Vec<u8>,
}

impl BinAccumulator {
    fn new(symbol_bytes: usize) -> Self {
        Self {
            degree: 0,
            xor_source_id: 0,
            xor_source_sig: 0,
            payload: vec![0; symbol_bytes],
        }
    }

    fn fold_source(&mut self, source_id: u64, source_sig: u64, payload: &[u8]) {
        self.degree += 1;
        self.xor_source_id ^= source_id;
        self.xor_source_sig ^= source_sig;
        xor_into(&mut self.payload, payload);
    }

    fn into_bin(self, bin_id: u64) -> MettleBin {
        MettleBin {
            bin_id,
            degree: self.degree,
            xor_source_id: self.xor_source_id,
            xor_source_sig: self.xor_source_sig,
            payload: self.payload,
        }
    }
}

impl Encoder {
    #[must_use]
    pub fn new(params: MettleParams) -> Self {
        Self {
            params,
            next_source_id: 0,
            total_bytes: 0,
            emitted_bins: Vec::new(),
            open_bins: BTreeMap::new(),
        }
    }

    pub fn push_source(&mut self, source_bytes: &[u8]) -> Vec<MettleBin> {
        let source_id = self.next_source_id;
        self.next_source_id += 1;
        self.total_bytes += source_bytes.len();

        let padded = pad_source_symbol(source_bytes, self.params.source_symbol_bytes);
        let source_sig = source_signature(self.params.seed, source_id);
        for bin_id in self.params.edges_for(source_id) {
            self.open_bins
                .entry(bin_id)
                .or_insert_with(|| BinAccumulator::new(self.params.source_symbol_bytes))
                .fold_source(source_id, source_sig, &padded);
        }

        let cutoff = self.params.base(self.next_source_id);
        let emitted = drain_before(&mut self.open_bins, cutoff);
        self.emitted_bins.extend(emitted.iter().cloned());
        emitted
    }

    pub fn finish(mut self) -> EncodedStream {
        let tail_bins = drain_before(&mut self.open_bins, u64::MAX);
        self.emitted_bins.extend(tail_bins);
        let bins = self.emitted_bins;
        let final_bin_exclusive = bins.last().map_or(0, |bin| bin.bin_id + 1);
        EncodedStream {
            params: self.params,
            total_bytes: self.total_bytes,
            total_sources: self.next_source_id,
            final_bin_exclusive,
            bins,
        }
    }

    #[must_use]
    pub fn encode_all(params: MettleParams, object: &[u8]) -> EncodedStream {
        let mut encoder = Self::new(params);
        let expected_sources = params.source_count_for_bytes(object.len());
        for chunk in object.chunks(params.source_symbol_bytes) {
            let _ = encoder.push_source(chunk);
        }
        let stream = encoder.finish();
        debug_assert_eq!(stream.total_sources, expected_sources);
        stream
    }
}

fn pad_source_symbol(source_bytes: &[u8], symbol_bytes: usize) -> Vec<u8> {
    let mut padded = vec![0; symbol_bytes];
    padded[..source_bytes.len()].copy_from_slice(source_bytes);
    padded
}

fn drain_before(open_bins: &mut BTreeMap<u64, BinAccumulator>, cutoff: u64) -> Vec<MettleBin> {
    let to_emit: Vec<u64> = open_bins
        .keys()
        .copied()
        .take_while(|bin_id| *bin_id < cutoff)
        .collect();
    let mut emitted = Vec::with_capacity(to_emit.len());
    for bin_id in to_emit {
        if let Some(bin) = open_bins.remove(&bin_id) {
            emitted.push(bin.into_bin(bin_id));
        }
    }
    emitted
}

pub(super) fn xor_into(dst: &mut [u8], src: &[u8]) {
    for (left, right) in dst.iter_mut().zip(src.iter().copied()) {
        *left ^= right;
    }
}


