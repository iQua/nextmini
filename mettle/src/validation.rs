use std::collections::BTreeSet;
use std::num::NonZeroUsize;

use crate::decoder::{DecodedSource, MettleDecoder};
use crate::encoder::{MettleBin, MettleEncoder};
use crate::{MettleParams, OverheadRatio};

fn small_source_stream() -> (MettleParams, NonZeroUsize, Vec<Vec<u8>>, Vec<MettleBin>) {
    let params = MettleParams::new(OverheadRatio::new(1, 20).expect("valid overhead"));
    let source_symbol_bytes = NonZeroUsize::new(2).expect("non-zero");
    let mut encoder = MettleEncoder::new(params, source_symbol_bytes, 0);
    let sources = vec![vec![1, 2], vec![3, 4], vec![5, 6]];
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
    bins: impl IntoIterator<Item = MettleBin>,
) -> Vec<DecodedSource> {
    let mut decoder = MettleDecoder::new(params, source_symbol_bytes, 0);
    let mut decoded = Vec::new();

    for bin in bins {
        decoded.extend(decoder.push_bin(bin));
    }

    decoded
}

fn decode_kept_bins(
    params: MettleParams,
    source_symbol_bytes: NonZeroUsize,
    bins: Vec<MettleBin>,
    erased_bin_ids: &BTreeSet<u128>,
) -> (MettleDecoder, Vec<DecodedSource>) {
    let mut decoder = MettleDecoder::new(params, source_symbol_bytes, 0);
    let mut decoded = Vec::new();

    for bin in bins {
        if erased_bin_ids.contains(&bin.bin_id()) {
            continue;
        }
        decoded.extend(decoder.push_bin(bin));
    }

    (decoder, decoded)
}

#[test]
fn validation_harness_round_trips_a_small_stream() {
    let (params, source_symbol_bytes, sources, bins) = small_source_stream();

    assert_eq!(
        decode_all_bins(params, source_symbol_bytes, bins)
            .iter()
            .map(DecodedSource::as_parts)
            .collect::<Vec<_>>(),
        vec![
            (0, sources[0].as_slice()),
            (1, sources[1].as_slice()),
            (2, sources[2].as_slice()),
        ]
    );
}

#[test]
fn validation_harness_stalls_until_a_missing_prefix_bin_is_replayed() {
    let (params, source_symbol_bytes, sources, bins) = small_source_stream();
    let missing_prefix_bin_id = params.tle_bin_id(0);
    let erased_bin_ids = BTreeSet::from([missing_prefix_bin_id]);
    let missing_prefix_bin = bins
        .iter()
        .find(|bin| bin.bin_id() == missing_prefix_bin_id)
        .expect("missing earliest prefix bin")
        .clone();
    let (mut decoder, mut decoded) =
        decode_kept_bins(params, source_symbol_bytes, bins, &erased_bin_ids);

    assert!(decoded.is_empty());

    decoded.extend(decoder.push_bin(missing_prefix_bin));
    assert_eq!(
        decoded.iter().map(DecodedSource::as_parts).collect::<Vec<_>>(),
        vec![
            (0, sources[0].as_slice()),
            (1, sources[1].as_slice()),
            (2, sources[2].as_slice()),
        ]
    );
}
