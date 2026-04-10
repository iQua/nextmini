//! In-memory simulation helpers for codec-only METTLE validation.

use std::collections::BTreeSet;

use super::decoder::{DecodedSource, Decoder, DecoderStats};
use super::encoder::EncodedStream;

#[derive(Debug, Clone)]
pub struct SimulationResult {
    pub recovered_object: Option<Vec<u8>>,
    pub decoded_sources: Vec<DecodedSource>,
    pub stats: DecoderStats,
}

#[must_use]
pub fn simulate_stream(stream: &EncodedStream, dropped_bins: &BTreeSet<u64>) -> SimulationResult {
    let mut decoder = Decoder::new(stream.params, stream.total_sources);
    let mut decoded_sources = Vec::new();
    for bin in stream.bins.iter().cloned() {
        if dropped_bins.contains(&bin.bin_id) {
            continue;
        }
        decoded_sources.extend(decoder.receive_bin(bin));
    }
    SimulationResult {
        recovered_object: decoder.rebuild_object(stream.total_bytes),
        decoded_sources,
        stats: decoder.stats(),
    }
}

