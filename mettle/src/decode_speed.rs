use std::num::NonZeroUsize;
use std::time::Instant;

use raptorq::{EncodingPacket, ObjectTransmissionInformation, SourceBlockDecoder, SourceBlockEncoder};

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

fn mettle_decode_fixture() -> (MettleParams, NonZeroUsize, Vec<MettleBin>) {
    let params = MettleParams::new(OverheadRatio::new(1, 20).expect("valid overhead"));
    let source_symbol_bytes = NonZeroUsize::new(BENCH_SYMBOL_SIZE).expect("non-zero");
    let mut encoder = MettleEncoder::new(params, source_symbol_bytes, 0);
    let mut bins = Vec::new();

    for source in benchmark_sources() {
        bins.extend(encoder.push_source(&source));
    }
    bins.extend(encoder.finish());

    (params, source_symbol_bytes, bins)
}

fn raptorq_decode_fixture() -> (ObjectTransmissionInformation, u64, Vec<EncodingPacket>, Vec<u8>) {
    let oti = ObjectTransmissionInformation::new(
        (BENCH_SOURCE_COUNT * BENCH_SYMBOL_SIZE) as u64,
        BENCH_SYMBOL_SIZE as u16,
        1,
        1,
        1,
    );
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

    (oti, flat_data.len() as u64, packets, flat_data)
}

#[test]
fn decode_speed_fixtures_build() {
    let (_, _, mettle_bins) = mettle_decode_fixture();
    let (oti, block_length, packets, flat_data) = raptorq_decode_fixture();
    let decoded = SourceBlockDecoder::new(0, &oti, block_length)
        .decode(packets)
        .expect("raptorq fixture should decode");

    assert!(!mettle_bins.is_empty());
    assert_eq!(decoded, flat_data);
    assert_eq!(decoded.len(), BENCH_SOURCE_COUNT * BENCH_SYMBOL_SIZE);
}

fn mettle_decode_once(
    params: MettleParams,
    source_symbol_bytes: NonZeroUsize,
    bins: &[MettleBin],
) -> usize {
    let mut decoder = MettleDecoder::new(params, source_symbol_bytes, 0);
    let mut decoded = Vec::new();

    for bin in bins.iter().cloned() {
        decoded.extend(decoder.push_bin(bin));
    }

    decoded.len()
}

fn raptorq_decode_once(
    oti: &ObjectTransmissionInformation,
    block_length: u64,
    packets: &[EncodingPacket],
) -> usize {
    SourceBlockDecoder::new(0, oti, block_length)
        .decode(packets.to_vec())
        .expect("raptorq fixture should decode")
        .len()
        / BENCH_SYMBOL_SIZE
}

#[test]
#[ignore = "manual decode-speed checkpoint"]
fn report_decode_speed_ratio() {
    const ITERATIONS: usize = 5;
    let (mettle_params, mettle_symbol_bytes, mettle_bins) = mettle_decode_fixture();
    let (raptorq_oti, raptorq_block_length, raptorq_packets, _) = raptorq_decode_fixture();

    let mettle_start = Instant::now();
    let mut mettle_decoded = 0;
    for _ in 0..ITERATIONS {
        mettle_decoded += mettle_decode_once(mettle_params, mettle_symbol_bytes, &mettle_bins);
    }
    let mettle_elapsed = mettle_start.elapsed();

    let raptorq_start = Instant::now();
    let mut raptorq_decoded = 0;
    for _ in 0..ITERATIONS {
        raptorq_decoded += raptorq_decode_once(&raptorq_oti, raptorq_block_length, &raptorq_packets);
    }
    let raptorq_elapsed = raptorq_start.elapsed();

    assert_eq!(mettle_decoded, ITERATIONS * BENCH_SOURCE_COUNT);
    assert_eq!(raptorq_decoded, ITERATIONS * BENCH_SOURCE_COUNT);

    let mettle_ns_per_source = mettle_elapsed.as_nanos() / (ITERATIONS as u128 * BENCH_SOURCE_COUNT as u128);
    let raptorq_ns_per_source =
        raptorq_elapsed.as_nanos() / (ITERATIONS as u128 * BENCH_SOURCE_COUNT as u128);
    let ratio = raptorq_ns_per_source as f64 / mettle_ns_per_source as f64;

    eprintln!(
        "mettle_ns_per_source={mettle_ns_per_source} raptorq_ns_per_source={raptorq_ns_per_source} ratio={ratio:.2}"
    );
}
