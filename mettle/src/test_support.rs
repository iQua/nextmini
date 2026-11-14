//! Testing-only accessors for METTLE paper-kernel internals.
//!
//! This module is `doc(hidden)` and not a stable production API. Lossless session
//! integration should use the future block/session adapters instead.

use std::num::NonZeroUsize;

use crate::MettleParams;
use crate::decoder::{DecodedSource, MettleDecoder, MettleDecoderStats};
use crate::encoder::{MettleBin, MettleEncoder};

#[doc(hidden)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SourceWindow {
    start: u128,
    end_exclusive: u128,
}

impl SourceWindow {
    pub const fn start(self) -> u128 {
        self.start
    }

    pub const fn end_exclusive(self) -> u128 {
        self.end_exclusive
    }

    pub const fn contains(self, bin_id: u128) -> bool {
        self.start <= bin_id && bin_id < self.end_exclusive
    }
}

#[doc(hidden)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DecoderStats {
    pub next_source_id: u64,
    pub terminal_source_count: Option<u64>,
    pub received_bins: usize,
    pub ready_bins: usize,
    pub seen_bins: usize,
    pub decoded_future_sources: usize,
    pub decoded_prefix_sources: usize,
    pub decoded_prefix_start_source_id: u64,
    pub graph_bins: Option<usize>,
    pub bin_cleanup_frontier: u128,
}

fn decoder_stats(stats: MettleDecoderStats) -> DecoderStats {
    DecoderStats {
        next_source_id: stats.next_source_id,
        terminal_source_count: stats.terminal_source_count,
        received_bins: stats.received_bins,
        ready_bins: stats.ready_bins,
        seen_bins: stats.seen_bins,
        decoded_future_sources: stats.decoded_future_sources,
        decoded_prefix_sources: stats.decoded_prefix_sources,
        decoded_prefix_start_source_id: stats.decoded_prefix_start_source_id,
        graph_bins: stats.graph_bins,
        bin_cleanup_frontier: stats.bin_cleanup_frontier,
    }
}

#[doc(hidden)]
pub struct Encoder(MettleEncoder);

impl Encoder {
    pub fn new(params: MettleParams, source_symbol_bytes: NonZeroUsize, seed: u64) -> Self {
        Self(MettleEncoder::new(params, source_symbol_bytes, seed))
    }

    pub fn new_terminated(
        params: MettleParams,
        source_symbol_bytes: NonZeroUsize,
        seed: u64,
        terminal_source_count: u64,
    ) -> Self {
        Self(MettleEncoder::new_terminated(
            params,
            source_symbol_bytes,
            seed,
            terminal_source_count,
        ))
    }

    pub fn push_source(&mut self, payload: &[u8]) -> Vec<(u128, Vec<u8>)> {
        self.0
            .push_source(payload)
            .into_iter()
            .map(MettleBin::into_parts)
            .collect()
    }

    pub fn finish(self) -> Vec<(u128, Vec<u8>)> {
        self.0
            .finish()
            .into_iter()
            .map(MettleBin::into_parts)
            .collect()
    }
}

#[doc(hidden)]
pub struct Decoder(MettleDecoder);

impl Decoder {
    pub fn new(params: MettleParams, source_symbol_bytes: NonZeroUsize, seed: u64) -> Self {
        Self(MettleDecoder::new(params, source_symbol_bytes, seed))
    }

    pub fn new_terminated(
        params: MettleParams,
        source_symbol_bytes: NonZeroUsize,
        seed: u64,
        terminal_source_count: u64,
    ) -> Self {
        Self(MettleDecoder::new_terminated(
            params,
            source_symbol_bytes,
            seed,
            terminal_source_count,
        ))
    }

    pub fn push_bin(&mut self, bin_id: u128, payload: Vec<u8>) -> Vec<(u64, Vec<u8>)> {
        self.0
            .push_bin(MettleBin::new(bin_id, payload))
            .into_iter()
            .map(DecodedSource::into_parts)
            .map(|(source_id, payload)| (source_id, payload.as_slice().to_vec()))
            .collect()
    }

    pub fn next_source_id(&self) -> u64 {
        self.0.next_source_id()
    }

    pub fn stats(&self) -> DecoderStats {
        decoder_stats(self.0.stats())
    }

    pub fn buffered_bin_remaining_touchers(&self, bin_id: u128) -> Option<u16> {
        self.0.buffered_bin_remaining_touchers(bin_id)
    }

    pub fn skip_next_source_without_edges(&mut self) -> usize {
        self.0.skip_next_source_without_edges().len()
    }
}

pub fn edge_bin_ids_with_terminal_source_count(
    params: MettleParams,
    source_id: u64,
    seed: u64,
    terminal_source_count: Option<u64>,
) -> [u128; MettleParams::EDGE_COUNT] {
    params.edge_bin_ids_with_terminal_source_count(source_id, seed, terminal_source_count)
}

pub fn source_window_with_terminal_source_count(
    params: MettleParams,
    source_id: u64,
    terminal_source_count: Option<u64>,
) -> SourceWindow {
    SourceWindow {
        start: params.tle_bin_id(source_id),
        end_exclusive: params
            .window_end_exclusive_with_terminal_source_count(source_id, terminal_source_count),
    }
}

pub fn tle_bin_id(params: MettleParams, source_id: u64) -> u128 {
    params.tle_bin_id(source_id)
}

pub fn terminal_departure_end_exclusive(params: MettleParams, terminal_source_count: u64) -> u128 {
    params.terminal_departure_end_exclusive(terminal_source_count)
}
