use std::num::NonZeroUsize;

use crate::decoder::{DecodedSource, MettleDecoder};
use crate::encoder::{MettleBin, MettleEncoder};
use crate::{MettleParams, OverheadRatio};

fn small_source_stream() -> (MettleParams, NonZeroUsize, Vec<Vec<u8>>, Vec<MettleBin>) {
    let params = MettleParams::new(OverheadRatio::new(1, 20).expect("valid overhead"));
    let source_symbol_bytes = NonZeroUsize::new(2).expect("non-zero");
    let sources = vec![vec![1, 2], vec![3, 4], vec![5, 6]];
    let mut encoder =
        MettleEncoder::new_terminated(params, source_symbol_bytes, 0, sources.len() as u64);
    let mut bins = Vec::new();

    for source in &sources {
        bins.extend(encoder.push_source(source));
    }
    bins.extend(encoder.finish());

    (params, source_symbol_bytes, sources, bins)
}

fn decode_all_bins(
    params: MettleParams,
    source_symbol_bytes: NonZeroUsize,
    source_count: usize,
    bins: impl IntoIterator<Item = MettleBin>,
) -> Vec<DecodedSource> {
    let mut decoder =
        MettleDecoder::new_terminated(params, source_symbol_bytes, 0, source_count as u64);
    let mut decoded = Vec::new();

    for bin in bins {
        decoded.extend(decoder.push_bin(bin));
    }

    decoded
}

fn expected_small_stream_decoded(sources: &[Vec<u8>]) -> [(u64, &[u8]); 3] {
    [
        (0, sources[0].as_slice()),
        (1, sources[1].as_slice()),
        (2, sources[2].as_slice()),
    ]
}

#[test]
fn validation_harness_round_trips_a_small_stream() {
    let (params, source_symbol_bytes, sources, bins) = small_source_stream();

    assert_eq!(
        decode_all_bins(params, source_symbol_bytes, sources.len(), bins)
            .iter()
            .map(DecodedSource::as_parts)
            .collect::<Vec<_>>(),
        expected_small_stream_decoded(&sources)
    );
}
