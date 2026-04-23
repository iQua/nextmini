use std::collections::BTreeMap;
use std::num::NonZeroUsize;
use std::sync::OnceLock;

use mettle::test_support::{
    Decoder as TestDecoder, Encoder as TestEncoder, edge_bin_ids_with_terminal_source_count,
    tle_bin_id,
};
use mettle::{MettleParams, OverheadRatio};

const SYMBOL_BYTES: usize = 1;
const SYSTEMATIC_SOURCE_COUNT: u64 = 640;

#[derive(Clone, Debug)]
struct SystematicFixture {
    params: MettleParams,
    source_symbol_bytes: NonZeroUsize,
    seed: u64,
    source_count: u64,
    sources: Vec<Vec<u8>>,
    fake_sources: Vec<Vec<u8>>,
    missing_source_id: u64,
    source_observation_payload: Vec<u8>,
    repair_bin_id: u128,
    repair_payload: Vec<u8>,
}

fn paper_params() -> MettleParams {
    MettleParams::new(OverheadRatio::new(1, 20).expect("valid overhead"))
}

fn systematic_sources(source_count: u64) -> Vec<Vec<u8>> {
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

fn bin_is_source_observation(params: MettleParams, source_count: u64, bin_id: u128) -> bool {
    (0..source_count).any(|source_id| tle_bin_id(params, source_id) == bin_id)
}

fn xor_payload(dst: &mut [u8], src: &[u8]) {
    for (dst_byte, src_byte) in dst.iter_mut().zip(src) {
        *dst_byte ^= *src_byte;
    }
}

fn fake_sources(
    params: MettleParams,
    seed: u64,
    source_count: u64,
    sources: &[Vec<u8>],
) -> Vec<Vec<u8>> {
    let mut fake_sources = Vec::<Vec<u8>>::with_capacity(sources.len());

    for source_id in 0..source_count {
        let mut fake_source = sources[source_id as usize].clone();
        let source_tle_bin_id = tle_bin_id(params, source_id);
        for previous_source_id in 0..source_id {
            if unique_edge_bin_ids(params, previous_source_id, seed, source_count)
                .contains(&source_tle_bin_id)
            {
                xor_payload(&mut fake_source, &fake_sources[previous_source_id as usize]);
            }
        }
        fake_sources.push(fake_source);
    }

    fake_sources
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

fn systematic_fixture() -> SystematicFixture {
    static FIXTURE: OnceLock<SystematicFixture> = OnceLock::new();

    FIXTURE.get_or_init(build_systematic_fixture).clone()
}

fn build_systematic_fixture() -> SystematicFixture {
    let params = paper_params();
    let source_symbol_bytes = NonZeroUsize::new(SYMBOL_BYTES).expect("non-zero");
    let source_count = SYSTEMATIC_SOURCE_COUNT;

    for seed in 0..64 {
        let sources = systematic_sources(source_count);
        let fake_sources = fake_sources(params, seed, source_count, &sources);
        let bins = encoded_bins(params, source_symbol_bytes, seed, source_count, &sources);

        for source_id in 1..source_count {
            if fake_sources[source_id as usize] == sources[source_id as usize] {
                continue;
            }
            for repair_bin_id in unique_edge_bin_ids(params, source_id, seed, source_count) {
                if bin_is_source_observation(params, source_count, repair_bin_id) {
                    continue;
                }
                if repair_touchers(params, seed, source_count, repair_bin_id) != vec![source_id] {
                    continue;
                }
                let repair_payload = bins
                    .get(&repair_bin_id)
                    .unwrap_or_else(|| panic!("missing repair bin {repair_bin_id}"))
                    .clone();
                let source_observation_payload = bins
                    .get(&tle_bin_id(params, source_id))
                    .unwrap_or_else(|| panic!("missing source bin for source {source_id}"))
                    .clone();

                return SystematicFixture {
                    params,
                    source_symbol_bytes,
                    seed,
                    source_count,
                    sources,
                    fake_sources,
                    missing_source_id: source_id,
                    source_observation_payload,
                    repair_bin_id,
                    repair_payload,
                };
            }
        }
    }

    panic!("expected a small systematic fixture with q_x != p_x and a unique repair bin");
}

fn push_raw_source(
    decoder: &mut TestDecoder,
    params: MettleParams,
    source_id: u64,
    payload: &[u8],
) -> Vec<(u64, Vec<u8>)> {
    decoder.push_bin(tle_bin_id(params, source_id), payload.to_vec())
}

#[test]
fn source_only_decode_accepts_all_raw_sources_with_zero_repair() {
    let params = paper_params();
    let source_symbol_bytes = NonZeroUsize::new(SYMBOL_BYTES).expect("non-zero");
    let source_count = 8u64;
    let sources = systematic_sources(source_count);
    let mut decoder = TestDecoder::new_terminated(params, source_symbol_bytes, 0, source_count);
    let mut decoded = Vec::new();

    for source_id in 0..source_count {
        decoded.extend(push_raw_source(
            &mut decoder,
            params,
            source_id,
            &sources[source_id as usize],
        ));
    }

    let expected = sources
        .into_iter()
        .enumerate()
        .map(|(source_id, payload)| (source_id as u64, payload))
        .collect::<Vec<_>>();
    assert_eq!(decoded, expected);
}

#[test]
fn repair_bins_are_computed_from_fake_q_not_raw_p() {
    let fixture = systematic_fixture();
    let source_index = fixture.missing_source_id as usize;

    assert_ne!(
        fixture.fake_sources[source_index],
        fixture.sources[source_index]
    );
    assert_eq!(
        fixture.source_observation_payload,
        fixture.sources[source_index]
    );
    assert_ne!(
        fixture.source_observation_payload,
        fixture.fake_sources[source_index]
    );
    assert_eq!(fixture.repair_payload, fixture.fake_sources[source_index]);
    assert_ne!(fixture.repair_payload, fixture.sources[source_index]);
}

#[test]
fn repair_assisted_decode_outputs_raw_p_not_fake_q() {
    let fixture = systematic_fixture();
    let mut decoder = TestDecoder::new_terminated(
        fixture.params,
        fixture.source_symbol_bytes,
        fixture.seed,
        fixture.source_count,
    );

    for source_id in 0..fixture.missing_source_id {
        let decoded = push_raw_source(
            &mut decoder,
            fixture.params,
            source_id,
            &fixture.sources[source_id as usize],
        );
        assert_eq!(decoded.len(), 1);
    }

    let decoded = decoder.push_bin(fixture.repair_bin_id, fixture.repair_payload.clone());
    let source_index = fixture.missing_source_id as usize;

    assert_eq!(
        decoded,
        vec![(
            fixture.missing_source_id,
            fixture.sources[source_index].clone()
        )]
    );
    assert_ne!(decoded[0].1, fixture.fake_sources[source_index]);
}

#[test]
fn out_of_order_repair_arrival_releases_output_in_source_order() {
    let fixture = systematic_fixture();
    let mut decoder = TestDecoder::new_terminated(
        fixture.params,
        fixture.source_symbol_bytes,
        fixture.seed,
        fixture.source_count,
    );

    assert!(
        decoder
            .push_bin(fixture.repair_bin_id, fixture.repair_payload.clone())
            .is_empty()
    );

    let mut decoded = Vec::new();
    for source_id in 0..fixture.missing_source_id {
        decoded.extend(push_raw_source(
            &mut decoder,
            fixture.params,
            source_id,
            &fixture.sources[source_id as usize],
        ));
    }

    let expected = fixture
        .sources
        .iter()
        .take(fixture.missing_source_id as usize + 1)
        .cloned()
        .enumerate()
        .map(|(source_id, payload)| (source_id as u64, payload))
        .collect::<Vec<_>>();
    assert_eq!(decoded, expected);
}

#[test]
fn out_of_order_raw_source_observation_derives_q_when_prefix_arrives() {
    let fixture = systematic_fixture();
    let source_index = fixture.missing_source_id as usize;
    let mut decoder = TestDecoder::new_terminated(
        fixture.params,
        fixture.source_symbol_bytes,
        fixture.seed,
        fixture.source_count,
    );

    assert_ne!(
        fixture.sources[source_index],
        fixture.fake_sources[source_index]
    );
    assert!(
        push_raw_source(
            &mut decoder,
            fixture.params,
            fixture.missing_source_id,
            &fixture.sources[source_index],
        )
        .is_empty()
    );

    let mut decoded = Vec::new();
    for source_id in 0..fixture.missing_source_id {
        decoded.extend(push_raw_source(
            &mut decoder,
            fixture.params,
            source_id,
            &fixture.sources[source_id as usize],
        ));
    }

    let expected = fixture
        .sources
        .iter()
        .take(source_index + 1)
        .cloned()
        .enumerate()
        .map(|(source_id, payload)| (source_id as u64, payload))
        .collect::<Vec<_>>();
    assert_eq!(decoded, expected);
}
