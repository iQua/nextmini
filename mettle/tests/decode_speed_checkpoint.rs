use std::num::NonZeroUsize;
use std::time::Instant;

use mettle::test_support::{Decoder as TestDecoder, Encoder as TestEncoder};
use mettle::{MettleParams, OverheadRatio};
use raptorq::{EncodingPacket, ObjectTransmissionInformation, SourceBlockDecoder, SourceBlockEncoder};

const BENCH_SYMBOL_SIZE: usize = 1500;
const TABLE_V_SOURCE_COUNTS: [usize; 7] = [127, 257, 511, 1002, 2040, 4069, 8194];

fn benchmark_sources(source_count: usize) -> Vec<Vec<u8>> {
    (0..source_count)
        .map(|source_id| {
            let mut payload = vec![0; BENCH_SYMBOL_SIZE];
            payload[0] = source_id as u8;
            payload
        })
        .collect()
}

fn benchmark_flat_data(source_count: usize) -> Vec<u8> {
    benchmark_sources(source_count).into_iter().flatten().collect()
}

fn expected_mettle_decode(source_count: usize) -> Vec<(u64, Vec<u8>)> {
    benchmark_sources(source_count)
        .into_iter()
        .enumerate()
        .map(|(source_id, payload)| (source_id as u64, payload))
        .collect()
}

fn mettle_decode_fixture(source_count: usize) -> (MettleParams, NonZeroUsize, Vec<(u128, Vec<u8>)>) {
    let params = MettleParams::new(OverheadRatio::new(1, 20).expect("valid overhead"));
    let source_symbol_bytes = NonZeroUsize::new(BENCH_SYMBOL_SIZE).expect("non-zero");
    let mut encoder =
        TestEncoder::new_terminated(params, source_symbol_bytes, 0, source_count as u64);
    let mut emitted_bins = Vec::new();

    for source in benchmark_sources(source_count) {
        emitted_bins.extend(encoder.push_source(&source));
    }
    emitted_bins.extend(encoder.finish());

    let mut decoder =
        TestDecoder::new_terminated(params, source_symbol_bytes, 0, source_count as u64);
    let mut decoded_sources = Vec::new();
    let mut completion_prefix = Vec::new();
    let expected = expected_mettle_decode(source_count);

    for (bin_id, payload) in emitted_bins {
        decoded_sources.extend(decoder.push_bin(bin_id, payload.clone()));
        completion_prefix.push((bin_id, payload));
        if decoded_sources.len() == source_count {
            assert_eq!(decoded_sources, expected);
            return (params, source_symbol_bytes, completion_prefix);
        }
    }

    panic!("METTLE fixture did not complete decode");
}

fn raptorq_decode_fixture(
    source_count: usize,
    target_packet_count: usize,
) -> (ObjectTransmissionInformation, u64, Vec<EncodingPacket>, Vec<u8>) {
    let oti = ObjectTransmissionInformation::new(
        (source_count * BENCH_SYMBOL_SIZE) as u64,
        BENCH_SYMBOL_SIZE as u16,
        1,
        1,
        1,
    );
    let flat_data = benchmark_flat_data(source_count);
    let encoder = SourceBlockEncoder::new(0, &oti, &flat_data);
    let source_packet_count = target_packet_count / 2;
    let repair_packet_count = target_packet_count.saturating_sub(source_packet_count);
    let mut packets = encoder
        .source_packets()
        .into_iter()
        .take(source_packet_count)
        .collect::<Vec<_>>();
    packets.extend(encoder.repair_packets(
        0,
        repair_packet_count
            .try_into()
            .expect("benchmark packet count fits in u32"),
    ));

    (oti, flat_data.len() as u64, packets, flat_data)
}

#[test]
fn decode_speed_fixtures_build() {
    let source_count = TABLE_V_SOURCE_COUNTS[0];
    let (_, _, mettle_bins) = mettle_decode_fixture(source_count);
    let (oti, block_length, packets, flat_data) =
        raptorq_decode_fixture(source_count, mettle_bins.len());
    let packet_count = packets.len();
    let decoded = SourceBlockDecoder::new(0, &oti, block_length)
        .decode(packets)
        .expect("raptorq fixture should decode");

    assert!(!mettle_bins.is_empty());
    assert!(mettle_bins.len() >= source_count);
    assert_eq!(packet_count, mettle_bins.len());
    assert_eq!(decoded, flat_data);
    assert_eq!(decoded.len(), source_count * BENCH_SYMBOL_SIZE);
}

fn mettle_decode_once(
    params: MettleParams,
    source_symbol_bytes: NonZeroUsize,
    source_count: usize,
    bins: Vec<(u128, Vec<u8>)>,
) -> usize {
    let mut decoder =
        TestDecoder::new_terminated(params, source_symbol_bytes, 0, source_count as u64);
    let mut decoded = 0;

    for (bin_id, payload) in bins {
        decoded += decoder.push_bin(bin_id, payload).len();
    }

    decoded
}

fn raptorq_decode_once(
    oti: &ObjectTransmissionInformation,
    block_length: u64,
    packets: Vec<EncodingPacket>,
) -> usize {
    SourceBlockDecoder::new(0, oti, block_length)
        .decode(packets)
        .expect("raptorq fixture should decode")
        .len()
        / BENCH_SYMBOL_SIZE
}

fn benchmark_decode_ratio(source_count: usize, iterations: usize) -> (usize, u128, u128, f64) {
    let (mettle_params, mettle_symbol_bytes, mettle_bins) = mettle_decode_fixture(source_count);
    let mettle_packet_count = mettle_bins.len();
    let (raptorq_oti, raptorq_block_length, raptorq_packets, _) =
        raptorq_decode_fixture(source_count, mettle_packet_count);
    let mettle_runs = (0..iterations)
        .map(|_| mettle_bins.clone())
        .collect::<Vec<_>>();
    let raptorq_runs = (0..iterations)
        .map(|_| raptorq_packets.clone())
        .collect::<Vec<_>>();

    let mettle_start = Instant::now();
    let mut mettle_decoded = 0;
    for bins in mettle_runs {
        mettle_decoded += mettle_decode_once(mettle_params, mettle_symbol_bytes, source_count, bins);
    }
    let mettle_elapsed = mettle_start.elapsed();

    let raptorq_start = Instant::now();
    let mut raptorq_decoded = 0;
    for packets in raptorq_runs {
        raptorq_decoded += raptorq_decode_once(&raptorq_oti, raptorq_block_length, packets);
    }
    let raptorq_elapsed = raptorq_start.elapsed();

    assert_eq!(mettle_decoded, iterations * source_count);
    assert_eq!(raptorq_decoded, iterations * source_count);

    let total_packets = iterations as u128 * mettle_packet_count as u128;
    let mettle_ns_per_packet = mettle_elapsed.as_nanos() / total_packets;
    let raptorq_ns_per_packet = raptorq_elapsed.as_nanos() / total_packets;
    let ratio = raptorq_ns_per_packet as f64 / mettle_ns_per_packet as f64;

    (mettle_packet_count, mettle_ns_per_packet, raptorq_ns_per_packet, ratio)
}

#[test]
#[ignore = "manual decode-speed checkpoint"]
fn report_decode_speed_ratio() {
    const ITERATIONS: usize = 5;
    for source_count in TABLE_V_SOURCE_COUNTS {
        let (packet_count, mettle_ns_per_packet, raptorq_ns_per_packet, ratio) =
            benchmark_decode_ratio(source_count, ITERATIONS);
        eprintln!(
            "k={source_count} packet_count={packet_count} mettle_ns_per_packet={mettle_ns_per_packet} raptorq_ns_per_packet={raptorq_ns_per_packet} ratio={ratio:.2}",
        );
    }
}
