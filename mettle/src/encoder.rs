#![cfg_attr(not(test), allow(dead_code))]

use std::num::NonZeroUsize;

use crate::MettleParams;

#[derive(Debug)]
pub(crate) struct MettleEncoder {
    pub(crate) params: MettleParams,
    pub(crate) source_symbol_bytes: NonZeroUsize,
    pub(crate) next_source_id: u64,
}

impl MettleEncoder {
    pub(crate) fn new(params: MettleParams, source_symbol_bytes: NonZeroUsize) -> Self {
        Self {
            params,
            source_symbol_bytes,
            next_source_id: 0,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroUsize;

    use crate::{MettleParams, OverheadRatio};

    use super::MettleEncoder;

    #[test]
    fn encoder_keeps_constructor_fields() {
        let params = MettleParams::new(OverheadRatio::new(1, 20).expect("valid overhead"));
        let encoder = MettleEncoder::new(params, NonZeroUsize::new(1500).expect("non-zero"));

        assert_eq!(encoder.params, params);
        assert_eq!(encoder.source_symbol_bytes.get(), 1500);
        assert_eq!(encoder.next_source_id, 0);
    }
}
