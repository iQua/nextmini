#![cfg_attr(not(test), allow(dead_code))]

use std::collections::BTreeMap;
use std::num::NonZeroUsize;

use crate::MettleParams;

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct MettleBin {
    bin_id: u128,
    payload: Vec<u8>,
}

#[derive(Debug)]
pub(crate) struct MettleEncoder {
    params: MettleParams,
    source_symbol_bytes: NonZeroUsize,
    next_source_id: u64,
    seed: u64,
    open_bins: BTreeMap<u128, Vec<u8>>,
}

impl MettleEncoder {
    pub(crate) fn new(params: MettleParams, source_symbol_bytes: NonZeroUsize, seed: u64) -> Self {
        Self {
            params,
            source_symbol_bytes,
            next_source_id: 0,
            seed,
            open_bins: BTreeMap::new(),
        }
    }

    pub(crate) fn push_source(&mut self, payload: &[u8]) -> Vec<MettleBin> {
        assert!(payload.len() <= self.source_symbol_bytes.get());

        let mut padded = vec![0; self.source_symbol_bytes.get()];
        padded[..payload.len()].copy_from_slice(payload);

        for bin_id in self.params.edge_bin_ids(self.next_source_id, self.seed) {
            let entry = self
                .open_bins
                .entry(bin_id)
                .or_insert_with(|| vec![0; self.source_symbol_bytes.get()]);
            xor_payload(entry, &padded);
        }

        self.next_source_id += 1;
        self.take_finalized_bins(self.params.tle_bin_id(self.next_source_id))
    }

    fn finish(mut self) -> Vec<MettleBin> {
        self.take_finalized_bins(u128::MAX)
    }

    fn take_finalized_bins(&mut self, end_exclusive: u128) -> Vec<MettleBin> {
        let future_bins = self.open_bins.split_off(&end_exclusive);
        let finalized_bins = std::mem::replace(&mut self.open_bins, future_bins);

        finalized_bins
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
        assert_eq!(encoder.seed, 7);
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
        let shared_bin = 482;
        let mut encoder = MettleEncoder::new(params, NonZeroUsize::new(1).expect("non-zero"), 0);

        let first_emitted = encoder.push_source(&[0b1010_0000]);
        assert_eq!(first_emitted, vec![MettleBin { bin_id: 0, payload: vec![0b1010_0000] }]);
        assert!(encoder.open_bins.contains_key(&shared_bin));

        let second_emitted = encoder.push_source(&[0b1100_0000]);

        assert!(!second_emitted.iter().any(|bin| bin.bin_id == shared_bin));
        assert_eq!(encoder.open_bins.get(&shared_bin), Some(&vec![0b0110_0000]));

        let release_source_id = (2..)
            .find(|&next_source_id| params.tle_bin_id(next_source_id) > shared_bin)
            .expect("future TLE frontier");
        let shared_bin_payload = (2..=release_source_id)
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
