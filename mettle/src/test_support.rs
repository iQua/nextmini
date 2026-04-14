use std::num::NonZeroUsize;

use crate::decoder::MettleDecoder;
use crate::decoder::DecodedSource;
use crate::encoder::{MettleBin, MettleEncoder};
use crate::MettleParams;

#[doc(hidden)]
pub struct Encoder(MettleEncoder);

impl Encoder {
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
