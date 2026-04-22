#![cfg_attr(not(test), allow(dead_code))]

use std::collections::BTreeMap;
use std::num::NonZeroUsize;

use crate::MettleParams;

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct MettleBin {
    bin_id: u128,
    payload: Vec<u8>,
}

impl MettleBin {
    pub(crate) fn new(bin_id: u128, payload: Vec<u8>) -> Self {
        Self { bin_id, payload }
    }

    pub(crate) fn into_parts(self) -> (u128, Vec<u8>) {
        (self.bin_id, self.payload)
    }
}

#[derive(Debug)]
pub(crate) struct MettleEncoder {
    params: MettleParams,
    source_symbol_bytes: NonZeroUsize,
    next_source_id: u64,
    next_departure_bin_id: u128,
    seed: u64,
    terminal_source_count: Option<u64>,
    open_bins: BTreeMap<u128, Vec<u8>>,
}

impl MettleEncoder {
    pub(crate) fn new(params: MettleParams, source_symbol_bytes: NonZeroUsize, seed: u64) -> Self {
        Self::new_with_terminal_source_count(params, source_symbol_bytes, seed, None)
    }

    pub(crate) fn new_terminated(
        params: MettleParams,
        source_symbol_bytes: NonZeroUsize,
        seed: u64,
        terminal_source_count: u64,
    ) -> Self {
        Self::new_with_terminal_source_count(
            params,
            source_symbol_bytes,
            seed,
            Some(terminal_source_count),
        )
    }

    fn new_with_terminal_source_count(
        params: MettleParams,
        source_symbol_bytes: NonZeroUsize,
        seed: u64,
        terminal_source_count: Option<u64>,
    ) -> Self {
        Self {
            params,
            source_symbol_bytes,
            next_source_id: 0,
            next_departure_bin_id: 0,
            seed,
            terminal_source_count,
            open_bins: BTreeMap::new(),
        }
    }

    pub(crate) fn push_source(&mut self, payload: &[u8]) -> Vec<MettleBin> {
        assert!(payload.len() <= self.source_symbol_bytes.get());
        if let Some(terminal_source_count) = self.terminal_source_count {
            assert!(self.next_source_id < terminal_source_count);
        }

        let mut padded = vec![0; self.source_symbol_bytes.get()];
        padded[..payload.len()].copy_from_slice(payload);

        for bin_id in self.edge_bin_ids(self.next_source_id) {
            let entry = self
                .open_bins
                .entry(bin_id)
                .or_insert_with(|| vec![0; self.source_symbol_bytes.get()]);
            xor_payload(entry, &padded);
        }

        self.next_source_id += 1;
        self.take_finalized_bins_until(
            self.params
                .departure_frontier_after_source_count(self.next_source_id),
        )
    }

    pub(crate) fn finish(mut self) -> Vec<MettleBin> {
        if let Some(terminal_source_count) = self.terminal_source_count {
            return self.take_finalized_bins_until(
                self.params
                    .terminal_departure_end_exclusive(terminal_source_count),
            );
        }

        self.flush_open_bins()
    }

    fn edge_bin_ids(&self, source_id: u64) -> [u128; MettleParams::EDGE_COUNT] {
        self.params.edge_bin_ids_with_terminal_source_count(
            source_id,
            self.seed,
            self.terminal_source_count,
        )
    }

    fn take_finalized_bins_until(&mut self, end_exclusive: u128) -> Vec<MettleBin> {
        let future_bins = self.open_bins.split_off(&end_exclusive);
        let mut finalized_bins = std::mem::replace(&mut self.open_bins, future_bins).into_iter();
        let mut next_finalized = finalized_bins.next();
        let mut emitted = Vec::new();

        for bin_id in self.next_departure_bin_id..end_exclusive {
            let payload = if next_finalized
                .as_ref()
                .is_some_and(|(finalized_bin_id, _)| *finalized_bin_id == bin_id)
            {
                let (_, payload) = next_finalized.take().expect("just matched finalized bin");
                next_finalized = finalized_bins.next();
                payload
            } else {
                vec![0; self.source_symbol_bytes.get()]
            };
            emitted.push(MettleBin { bin_id, payload });
        }

        self.next_departure_bin_id = end_exclusive;
        emitted
    }

    fn flush_open_bins(&mut self) -> Vec<MettleBin> {
        let remaining_bins = std::mem::take(&mut self.open_bins);

        remaining_bins
            .into_iter()
            .map(|(bin_id, payload)| MettleBin { bin_id, payload })
            .collect()
    }
}

fn xor_payload(dst: &mut [u8], src: &[u8]) {
    for (dst_byte, src_byte) in dst.iter_mut().zip(src) {
        *dst_byte ^= *src_byte;
    }
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroUsize;

    use crate::{MettleParams, OverheadRatio};

    use super::{MettleBin, MettleEncoder};

    #[test]
    fn encoder_keeps_constructor_fields() {
        let params = MettleParams::new(OverheadRatio::new(1, 20).expect("valid overhead"));
        let encoder = MettleEncoder::new(params, NonZeroUsize::new(1500).expect("non-zero"), 7);

        assert_eq!(encoder.params, params);
        assert_eq!(encoder.source_symbol_bytes.get(), 1500);
        assert_eq!(encoder.next_source_id, 0);
        assert_eq!(encoder.next_departure_bin_id, 0);
        assert_eq!(encoder.seed, 7);
        assert_eq!(encoder.terminal_source_count, None);
        assert!(encoder.open_bins.is_empty());
    }

    #[test]
    fn push_source_emits_finalized_tle_bin() {
        let params = MettleParams::new(OverheadRatio::new(1, 20).expect("valid overhead"));
        let mut encoder =
            MettleEncoder::new(params, NonZeroUsize::new(4).expect("non-zero"), 0x1234);

        let emitted = encoder.push_source(&[1, 2]);

        assert_eq!(
            emitted,
            vec![MettleBin {
                bin_id: 0,
                payload: vec![1, 2, 0, 0],
            }]
        );
        assert_eq!(encoder.next_source_id, 1);
    }

    #[test]
    fn push_source_keeps_overlapping_future_bin_open() {
        let params = MettleParams::new(OverheadRatio::new(1, 20).expect("valid overhead"));
        let (first_source_id, second_source_id, shared_bin) = (0..64)
            .find_map(|first_source_id| {
                let first_edges = params.edge_bin_ids(first_source_id, 0);
                ((first_source_id + 1)..64).find_map(|second_source_id| {
                    let second_edges = params.edge_bin_ids(second_source_id, 0);
                    first_edges
                        .into_iter()
                        .find(|&bin_id| {
                            bin_id >= params.tle_bin_id(second_source_id + 1)
                                && second_edges.contains(&bin_id)
                        })
                        .map(|bin_id| (first_source_id, second_source_id, bin_id))
                })
            })
            .expect("expected a future overlapping bin");
        let mut encoder = MettleEncoder::new(params, NonZeroUsize::new(1).expect("non-zero"), 0);

        for _ in 0..first_source_id {
            encoder.push_source(&[0]);
        }

        encoder.push_source(&[0b1010_0000]);
        assert!(encoder.open_bins.contains_key(&shared_bin));

        for _ in (first_source_id + 1)..second_source_id {
            encoder.push_source(&[0]);
        }

        let second_emitted = encoder.push_source(&[0b1100_0000]);

        assert!(!second_emitted.iter().any(|bin| bin.bin_id == shared_bin));
        assert_eq!(encoder.open_bins.get(&shared_bin), Some(&vec![0b0110_0000]));

        let release_source_id = ((second_source_id + 1)..)
            .find(|&next_source_id| params.tle_bin_id(next_source_id) > shared_bin)
            .expect("future TLE frontier");
        let shared_bin_payload = ((second_source_id + 1)..=release_source_id)
            .find_map(|_| {
                let emitted = encoder.push_source(&[0]);
                emitted
                    .into_iter()
                    .find(|bin| bin.bin_id == shared_bin)
                    .map(|bin| bin.payload)
            })
            .unwrap_or_else(|| panic!("shared bin never emitted"));

        assert_eq!(shared_bin_payload, vec![0b0110_0000]);
    }

    #[test]
    fn terminated_encoder_releases_dense_departure_prefix() {
        let params = MettleParams::new(OverheadRatio::new(1, 20).expect("valid overhead"));
        let source_symbol_bytes = NonZeroUsize::new(1).expect("non-zero");
        let mut encoder = MettleEncoder::new_terminated(params, source_symbol_bytes, 0, 20);
        let mut emitted_bin_ids = Vec::new();

        for _ in 0..20 {
            emitted_bin_ids.extend(
                encoder
                    .push_source(&[1])
                    .into_iter()
                    .map(|bin| bin.bin_id),
            );
        }

        assert_eq!(
            emitted_bin_ids,
            (0..params.departure_frontier_after_source_count(20)).collect::<Vec<_>>()
        );
    }

    #[test]
    fn finish_emits_remaining_open_bins_in_order() {
        let params = MettleParams::new(OverheadRatio::new(1, 20).expect("valid overhead"));
        let mut encoder = MettleEncoder::new(params, NonZeroUsize::new(1).expect("non-zero"), 0);
        let mut expected_remaining_bin_ids = params.edge_bin_ids(0, 0);
        expected_remaining_bin_ids.sort_unstable();

        let emitted = encoder.push_source(&[0b1010_0000]);
        let remaining = encoder.finish();

        assert_eq!(
            emitted,
            vec![MettleBin {
                bin_id: expected_remaining_bin_ids[0],
                payload: vec![0b1010_0000],
            }]
        );
        assert_eq!(remaining.len(), expected_remaining_bin_ids.len() - 1);
        assert_eq!(
            remaining.iter().map(|bin| bin.bin_id).collect::<Vec<_>>(),
            expected_remaining_bin_ids[1..]
        );
        assert!(remaining.iter().all(|bin| bin.payload == vec![0b1010_0000]));
    }
}
