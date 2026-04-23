//! Testing-only accessors for METTLE paper-kernel internals.
//!
//! This module is `doc(hidden)` and not a stable production API. Lossless session
//! integration should use the future block/session adapters instead.

use std::num::NonZeroUsize;

use crate::decoder::MettleDecoder;
use crate::decoder::DecodedSource;
use crate::encoder::{MettleBin, MettleEncoder};
use crate::MettleParams;

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
pub struct Encoder(MettleEncoder);

impl Encoder {
    pub fn new(
        params: MettleParams,
        source_symbol_bytes: NonZeroUsize,
        seed: u64,
    ) -> Self {
        Self(MettleEncoder::new(
            params,
            source_symbol_bytes,
            seed,
        ))
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
    pub fn new(
        params: MettleParams,
        source_symbol_bytes: NonZeroUsize,
        seed: u64,
    ) -> Self {
        Self(MettleDecoder::new(
            params,
            source_symbol_bytes,
            seed,
        ))
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
            .collect()
    }

    pub fn next_source_id(&self) -> u64 {
        self.0.next_source_id()
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

pub fn terminal_departure_end_exclusive(
    params: MettleParams,
    terminal_source_count: u64,
) -> u128 {
    params.terminal_departure_end_exclusive(terminal_source_count)
}
