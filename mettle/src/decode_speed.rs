use std::num::NonZeroUsize;

use raptorq::{
    EncodingPacket, ObjectTransmissionInformation, SourceBlockDecoder, SourceBlockEncoder,
};

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

fn benchmark_flat_data() -> Vec<u8> {
    benchmark_sources().into_iter().flatten().collect()
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

fn raptorq_decode_fixture() -> (ObjectTransmissionInformation, u64, Vec<EncodingPacket>) {
    let oti = ObjectTransmissionInformation::new(
        (BENCH_SOURCE_COUNT * BENCH_SYMBOL_SIZE) as u64,
        BENCH_SYMBOL_SIZE as u16,
        1,
        1,
        1,
    );
    let block_length = (BENCH_SOURCE_COUNT * BENCH_SYMBOL_SIZE) as u64;
    let flat_data = benchmark_flat_data();
    let encoder = SourceBlockEncoder::new(0, &oti, &flat_data);
    let half_source_count = BENCH_SOURCE_COUNT / 2;
    let mut packets = encoder.source_packets().into_iter().take(half_source_count).collect::<Vec<_>>();
    packets.extend(encoder.repair_packets(
        0,
        (BENCH_SOURCE_COUNT - half_source_count)
            .try_into()
            .expect("benchmark source count fits in u32"),
    ));

    (oti, block_length, packets)
}

#[test]
fn decode_speed_fixtures_build() {
    let (_, mettle_bins) = mettle_decode_fixture();
    let (oti, block_length, packets) = raptorq_decode_fixture();
    let decoded = SourceBlockDecoder::new(0, &oti, block_length)
        .decode(packets)
        .expect("raptorq fixture should decode");

    assert!(!mettle_bins.is_empty());
    assert_eq!(decoded, benchmark_flat_data());
    assert_eq!(decoded.len(), BENCH_SOURCE_COUNT * BENCH_SYMBOL_SIZE);
}
