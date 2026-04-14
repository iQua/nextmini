#![cfg_attr(not(test), allow(dead_code))]

use std::num::NonZeroUsize;

use crate::MettleParams;

#[derive(Debug)]
pub(crate) struct MettleDecoder {
    params: MettleParams,
    source_symbol_bytes: NonZeroUsize,
    next_decoded_source_id: u64,
    seed: u64,
}

impl MettleDecoder {
    pub(crate) fn new(params: MettleParams, source_symbol_bytes: NonZeroUsize, seed: u64) -> Self {
        Self {
            params,
            source_symbol_bytes,
            next_decoded_source_id: 0,
            seed,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroUsize;

    use crate::{MettleParams, OverheadRatio};

    use super::MettleDecoder;

    #[test]
    fn decoder_keeps_constructor_fields() {
        let params = MettleParams::new(OverheadRatio::new(1, 20).expect("valid overhead"));
        let decoder = MettleDecoder::new(params, NonZeroUsize::new(1500).expect("non-zero"), 7);

        assert_eq!(decoder.params, params);
        assert_eq!(decoder.source_symbol_bytes.get(), 1500);
        assert_eq!(decoder.next_decoded_source_id, 0);
        assert_eq!(decoder.seed, 7);
    }
}
