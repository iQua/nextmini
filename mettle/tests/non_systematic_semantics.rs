use std::collections::BTreeMap;
use std::num::NonZeroUsize;

use mettle::test_support::{
    Decoder as TestDecoder, Encoder as TestEncoder, edge_bin_ids_with_terminal_source_count,
    tle_bin_id,
};
use mettle::{MettleParams, OverheadRatio};

const SYMBOL_BYTES: usize = 1;
const SOURCE_COUNT: u64 = 640;

fn paper_params() -> MettleParams {
    MettleParams::new(OverheadRatio::new(1, 20).expect("valid overhead"))
}

fn sources(source_count: u64) -> Vec<Vec<u8>> {
    (0..source_count)
        .map(|source_id| vec![(source_id as u8).wrapping_mul(37).wrapping_add(0x51)])
        .collect()
}

fn unique_edge_bin_ids(
    params: MettleParams,
    source_id: u64,
    seed: u64,
    source_count: u64,
) -> Vec<u128> {
    let mut edge_bin_ids =
        edge_bin_ids_with_terminal_source_count(params, source_id, seed, Some(source_count));
    edge_bin_ids.sort_unstable();

    let mut unique_edge_bin_ids = Vec::with_capacity(MettleParams::EDGE_COUNT);
    for bin_id in edge_bin_ids {
        if unique_edge_bin_ids.last() != Some(&bin_id) {
            unique_edge_bin_ids.push(bin_id);
        }
    }

    unique_edge_bin_ids
}

fn encoded_bins(
    params: MettleParams,
    source_symbol_bytes: NonZeroUsize,
    seed: u64,
    source_count: u64,
    sources: &[Vec<u8>],
) -> BTreeMap<u128, Vec<u8>> {
    let mut encoder = TestEncoder::new_terminated(params, source_symbol_bytes, seed, source_count);
    let mut bins = BTreeMap::new();

    for source in sources {
        for (bin_id, payload) in encoder.push_source(source) {
            assert!(bins.insert(bin_id, payload).is_none());
        }
    }
    for (bin_id, payload) in encoder.finish() {
        assert!(bins.insert(bin_id, payload).is_none());
    }

    bins
}

fn repair_touchers(
    params: MettleParams,
    seed: u64,
    source_count: u64,
    repair_bin_id: u128,
) -> Vec<u64> {
    (0..source_count)
        .filter(|&source_id| {
            unique_edge_bin_ids(params, source_id, seed, source_count).contains(&repair_bin_id)
        })
        .collect()
}

fn push_bin(decoder: &mut TestDecoder, bin_id: u128, payload: &[u8]) -> Vec<(u64, Vec<u8>)> {
    decoder.push_bin(bin_id, payload.to_vec())
}

#[test]
fn no_loss_tle_prefix_decodes_raw_sources() {
    let params = paper_params();
    let source_symbol_bytes = NonZeroUsize::new(SYMBOL_BYTES).expect("non-zero");
    let source_count = 8u64;
    let source_payloads = sources(source_count);
    let bins = encoded_bins(
        params,
        source_symbol_bytes,
        0,
        source_count,
        &source_payloads,
    );
    let mut decoder = TestDecoder::new_terminated(params, source_symbol_bytes, 0, source_count);
    let mut decoded = Vec::new();

    for source_id in 0..source_count {
        let bin_id = tle_bin_id(params, source_id);
        decoded.extend(push_bin(
            &mut decoder,
            bin_id,
            bins.get(&bin_id)
                .unwrap_or_else(|| panic!("missing TLE bin {bin_id}")),
        ));
    }

    let expected = source_payloads
        .into_iter()
        .enumerate()
        .map(|(source_id, payload)| (source_id as u64, payload))
        .collect::<Vec<_>>();
    assert_eq!(decoded, expected);
}

#[test]
fn tle_bins_are_still_coded_equations_when_prior_sources_touch_them() {
    let params = paper_params();
    let source_symbol_bytes = NonZeroUsize::new(SYMBOL_BYTES).expect("non-zero");
    let source_count = SOURCE_COUNT;
    let seed = 0;
    let source_payloads = sources(source_count);
    let bins = encoded_bins(
        params,
        source_symbol_bytes,
        seed,
        source_count,
        &source_payloads,
    );

    let source_id = (1..source_count)
        .find(|&source_id| {
            let source_tle = tle_bin_id(params, source_id);
            (0..source_id).any(|previous_source_id| {
                unique_edge_bin_ids(params, previous_source_id, seed, source_count)
                    .contains(&source_tle)
            })
        })
        .expect("fixture with prior source touching a later TLE bin");
    let tle_bin = tle_bin_id(params, source_id);

    assert_ne!(
        bins.get(&tle_bin).expect("TLE bin payload"),
        &source_payloads[source_id as usize],
        "paper-native non-systematic METTLE does not rewrite TLE bins into raw source packets"
    );
}

#[test]
fn unique_repair_bin_is_raw_source_xor_not_systematic_q_transform() {
    let params = paper_params();
    let source_symbol_bytes = NonZeroUsize::new(SYMBOL_BYTES).expect("non-zero");
    let source_count = SOURCE_COUNT;
    let source_payloads = sources(source_count);

    for seed in 0..64 {
        let bins = encoded_bins(
            params,
            source_symbol_bytes,
            seed,
            source_count,
            &source_payloads,
        );
        for source_id in 1..source_count {
            for repair_bin_id in unique_edge_bin_ids(params, source_id, seed, source_count) {
                if repair_bin_id == tle_bin_id(params, source_id) {
                    continue;
                }
                if repair_touchers(params, seed, source_count, repair_bin_id) != vec![source_id] {
                    continue;
                }

                assert_eq!(
                    bins.get(&repair_bin_id)
                        .unwrap_or_else(|| panic!("missing repair bin {repair_bin_id}")),
                    &source_payloads[source_id as usize]
                );
                return;
            }
        }
    }

    panic!("expected a unique repair bin fixture");
}
