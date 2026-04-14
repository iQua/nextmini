use std::num::NonZeroUsize;

use raptorq::{ObjectTransmissionInformation, SourceBlockEncoder};

use crate::decoder::MettleDecoder;
use crate::encoder::{MettleBin, MettleEncoder};
use crate::{MettleParams, OverheadRatio};

const BENCH_SOURCE_COUNT: usize = 32;
const BENCH_SYMBOL_SIZE: usize = 64;

fn benchmark_sources() -> Vec<Vec<u8>> {
    (0..BENCH_SOURCE_COUNT)
        .map(|source_id| {
            let mut payload = vec![0; BENCH_SYMBOL_SIZE];
            payload[0] = source_id as u8;
            payload
        })
        .collect()
}

fn mettle_decode_fixture() -> (MettleDecoder, Vec<MettleBin>) {
    let params = MettleParams::new(OverheadRatio::new(1, 20).expect("valid overhead"));
    let source_symbol_bytes = NonZeroUsize::new(BENCH_SYMBOL_SIZE).expect("non-zero");
    let mut encoder = MettleEncoder::new(params, source_symbol_bytes, 0);
    let mut bins = Vec::new();

    for source in benchmark_sources() {
        bins.extend(encoder.push_source(&source));
    }
    bins.extend(encoder.finish());

    (MettleDecoder::new(params, source_symbol_bytes, 0), bins)
}

fn raptorq_decode_fixture() -> usize {
    let oti = ObjectTransmissionInformation::new(
        (BENCH_SOURCE_COUNT * BENCH_SYMBOL_SIZE) as u64,
        BENCH_SYMBOL_SIZE as u16,
        1,
        1,
        1,
    );
    let flat_data = benchmark_sources().into_iter().flatten().collect::<Vec<_>>();
    let encoder = SourceBlockEncoder::new(0, &oti, &flat_data);

    encoder.source_packets().len()
}

#[test]
fn decode_speed_fixtures_build() {
    let (_, mettle_bins) = mettle_decode_fixture();

    assert!(!mettle_bins.is_empty());
    assert_eq!(raptorq_decode_fixture(), BENCH_SOURCE_COUNT);
}
