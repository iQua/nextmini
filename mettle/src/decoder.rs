#![cfg_attr(not(test), allow(dead_code))]

use std::collections::BTreeMap;
use std::num::NonZeroUsize;

use crate::encoder::MettleBin;
use crate::MettleParams;

#[derive(Debug)]
pub(crate) struct MettleDecoder {
    params: MettleParams,
    source_symbol_bytes: NonZeroUsize,
    next_decoded_source_id: u64,
    seed: u64,
    received_bins: BTreeMap<u128, Vec<u8>>,
}

impl MettleDecoder {
    pub(crate) fn new(params: MettleParams, source_symbol_bytes: NonZeroUsize, seed: u64) -> Self {
        Self {
            params,
            source_symbol_bytes,
            next_decoded_source_id: 0,
            seed,
            received_bins: BTreeMap::new(),
        }
    }

    pub(crate) fn push_bin(&mut self, bin: MettleBin) -> bool {
        let (bin_id, payload) = bin.into_parts();
        if payload.len() != self.source_symbol_bytes.get() {
            return false;
        }
        self.received_bins.entry(bin_id).or_insert(payload);
        true
    }
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroUsize;

    use crate::{MettleParams, OverheadRatio};

    use crate::encoder::MettleBin;

    use super::MettleDecoder;

    #[test]
    fn decoder_keeps_constructor_fields() {
        let params = MettleParams::new(OverheadRatio::new(1, 20).expect("valid overhead"));
        let decoder = MettleDecoder::new(params, NonZeroUsize::new(1500).expect("non-zero"), 7);

        assert_eq!(decoder.params, params);
        assert_eq!(decoder.source_symbol_bytes.get(), 1500);
        assert_eq!(decoder.next_decoded_source_id, 0);
        assert_eq!(decoder.seed, 7);
        assert!(decoder.received_bins.is_empty());
    }

    #[test]
    fn push_bin_deduplicates_by_bin_id() {
        let params = MettleParams::new(OverheadRatio::new(1, 20).expect("valid overhead"));
        let mut decoder =
            MettleDecoder::new(params, NonZeroUsize::new(4).expect("non-zero"), 0x1234);

        assert!(decoder.push_bin(MettleBin::new(17, vec![1, 2, 3, 4])));
        assert!(decoder.push_bin(MettleBin::new(17, vec![1, 2, 3, 4])));

        assert_eq!(decoder.received_bins.len(), 1);
        assert_eq!(decoder.received_bins.get(&17), Some(&vec![1, 2, 3, 4]));
    }

    #[test]
    fn push_bin_rejects_wrong_payload_length() {
        let params = MettleParams::new(OverheadRatio::new(1, 20).expect("valid overhead"));
        let mut decoder =
            MettleDecoder::new(params, NonZeroUsize::new(4).expect("non-zero"), 0x1234);

        assert!(!decoder.push_bin(MettleBin::new(17, vec![1, 2, 3])));
        assert!(decoder.received_bins.is_empty());
    }
}
