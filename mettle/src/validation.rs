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
    bins: impl IntoIterator<Item = MettleBin>,
) -> (MettleDecoder, Vec<DecodedSource>) {
    let mut decoder = MettleDecoder::new(params, source_symbol_bytes, 0);
    let mut decoded = Vec::new();

    for bin in bins {
        decoded.extend(decoder.push_bin(bin));
    }

    (decoder, decoded)
}

fn split_out_bin(
    bins: impl IntoIterator<Item = MettleBin>,
    selected_bin_id: u128,
) -> (Vec<MettleBin>, MettleBin) {
    let mut kept_bins = Vec::new();
    let mut selected_bin = None;

    for bin in bins {
        if bin.bin_id() == selected_bin_id {
            assert!(selected_bin.is_none(), "selected bin id must be unique");
            selected_bin = Some(bin);
        } else {
            kept_bins.push(bin);
        }
    }

    (kept_bins, selected_bin.expect("selected bin"))
}

fn expected_small_stream_decoded(sources: &[Vec<u8>]) -> Vec<(u64, &[u8])> {
    vec![
        (0, sources[0].as_slice()),
        (1, sources[1].as_slice()),
        (2, sources[2].as_slice()),
    ]
}

#[test]
fn validation_harness_round_trips_a_small_stream() {
    let (params, source_symbol_bytes, sources, bins) = small_source_stream();

    assert_eq!(
        decode_all_bins(params, source_symbol_bytes, bins)
            .iter()
            .map(DecodedSource::as_parts)
            .collect::<Vec<_>>(),
        expected_small_stream_decoded(&sources)
    );
}

#[test]
fn validation_harness_stalls_until_a_missing_prefix_bin_is_replayed() {
    let (params, source_symbol_bytes, sources, bins) = small_source_stream();
    let missing_prefix_bin_id = params.tle_bin_id(0);
    let (kept_bins, missing_prefix_bin) = split_out_bin(bins, missing_prefix_bin_id);
    let (mut decoder, mut decoded) = decode_kept_bins(params, source_symbol_bytes, kept_bins);

    assert!(decoded.is_empty());

    decoded.extend(decoder.push_bin(missing_prefix_bin));
    assert_eq!(
        decoded.iter().map(DecodedSource::as_parts).collect::<Vec<_>>(),
        expected_small_stream_decoded(&sources)
    );
}

#[test]
fn validation_harness_matches_small_stream_bin_goldens() {
    let (_, _, _, bins) = small_source_stream();

    assert_eq!(
        bins.into_iter().map(MettleBin::into_parts).collect::<Vec<_>>(),
        vec![
            (0, vec![1, 2]),
            (1, vec![3, 4]),
            (2, vec![5, 6]),
            (332, vec![3, 4]),
            (334, vec![5, 6]),
            (336, vec![1, 2]),
            (482, vec![2, 6]),
            (494, vec![5, 6]),
            (547, vec![1, 2]),
            (556, vec![3, 4]),
            (570, vec![5, 6]),
        ]
    );
}

#[test]
fn validation_harness_matches_kept_bin_goldens_after_prefix_erasure() {
    let (params, _, _, bins) = small_source_stream();
    let (kept_bins, _) = split_out_bin(bins, params.tle_bin_id(0));

    assert_eq!(
        kept_bins
            .into_iter()
            .map(MettleBin::into_parts)
            .collect::<Vec<_>>(),
        vec![
            (1, vec![3, 4]),
            (2, vec![5, 6]),
            (332, vec![3, 4]),
            (334, vec![5, 6]),
            (336, vec![1, 2]),
            (482, vec![2, 6]),
            (494, vec![5, 6]),
            (547, vec![1, 2]),
            (556, vec![3, 4]),
            (570, vec![5, 6]),
        ]
    );
}
