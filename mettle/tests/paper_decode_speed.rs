use std::num::NonZeroUsize;
use std::time::Instant;

use mettle::test_support::{Decoder as TestDecoder, Encoder as TestEncoder};
use mettle::{MettleParams, OverheadRatio};
use raptorq::{EncodingPacket, ObjectTransmissionInformation, SourceBlockDecoder, SourceBlockEncoder};

const PAPER_SPEED_SYMBOL_SIZE: usize = 1500;
const PAPER_SPEED_METTLE_SOURCE_COUNT: usize = 100_000;
const PAPER_SPEED_SEED: u64 = 0;

const TABLE_IV_LATENCY_MATCHED_KS: [usize; 9] = [84, 101, 114, 149, 168, 236, 257, 269, 405];
const TABLE_V_RAPTORQ_ROWS: [(usize, u32); 7] = [
    (127, 122),
    (257, 150),
    (511, 265),
    (1002, 616),
    (2040, 3545),
    (4069, 5824),
    (8194, 21451),
];

fn benchmark_sources(source_count: usize) -> Vec<Vec<u8>> {
    (0..source_count)
        .map(|source_id| {
            let mut payload = vec![0; PAPER_SPEED_SYMBOL_SIZE];
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

fn mettle_streaming_fixture(
    source_count: usize,
) -> (MettleParams, NonZeroUsize, Vec<(u128, Vec<u8>)>) {
    let params = MettleParams::new(OverheadRatio::new(1, 20).expect("valid overhead"));
    let source_symbol_bytes = NonZeroUsize::new(PAPER_SPEED_SYMBOL_SIZE).expect("non-zero");
    let mut encoder = TestEncoder::new(params, source_symbol_bytes, PAPER_SPEED_SEED);
    let mut emitted_bins = Vec::new();

    for source in benchmark_sources(source_count) {
        emitted_bins.extend(encoder.push_source(&source));
    }

    let mut decoder = TestDecoder::new(params, source_symbol_bytes, PAPER_SPEED_SEED);
    let mut decoded_sources = Vec::new();
    for (bin_id, payload) in emitted_bins.iter().cloned() {
        decoded_sources.extend(decoder.push_bin(bin_id, payload));
    }

    assert_eq!(decoder.next_source_id(), source_count as u64);
    assert_eq!(decoded_sources, expected_mettle_decode(source_count));

    (params, source_symbol_bytes, emitted_bins)
}

fn raptorq_repair_only_fixture(
    source_count: usize,
) -> (ObjectTransmissionInformation, u64, Vec<EncodingPacket>) {
    let oti = ObjectTransmissionInformation::new(
        (source_count * PAPER_SPEED_SYMBOL_SIZE) as u64,
        PAPER_SPEED_SYMBOL_SIZE as u16,
        1,
        1,
        1,
    );
    let flat_data = benchmark_flat_data(source_count);
    let encoder = SourceBlockEncoder::new(0, &oti, &flat_data);
    let packets = encoder.repair_packets(0, source_count as u32);

    let decoded = SourceBlockDecoder::new(0, &oti, flat_data.len() as u64)
        .decode(packets.clone())
        .expect("repair-only RaptorQ fixture should decode");
    assert_eq!(decoded, flat_data);

    (oti, flat_data.len() as u64, packets)
}

fn mettle_decode_once(
    params: MettleParams,
    source_symbol_bytes: NonZeroUsize,
    source_count: usize,
    bins: Vec<(u128, Vec<u8>)>,
) -> usize {
    let mut decoder = TestDecoder::new(params, source_symbol_bytes, PAPER_SPEED_SEED);
    let mut decoded = 0;

    for (bin_id, payload) in bins {
        decoded += decoder.push_bin(bin_id, payload).len();
    }

    assert_eq!(decoder.next_source_id(), source_count as u64);
    decoded
}

fn raptorq_decode_once(
    oti: &ObjectTransmissionInformation,
    block_length: u64,
    packets: Vec<EncodingPacket>,
) -> usize {
    SourceBlockDecoder::new(0, oti, block_length)
        .decode(packets)
        .expect("paper-style RaptorQ fixture should decode")
        .len()
        / PAPER_SPEED_SYMBOL_SIZE
}

fn benchmark_mettle_ns_per_packet(iterations: usize, source_count: usize) -> (usize, u128) {
    let (params, source_symbol_bytes, bins) = mettle_streaming_fixture(source_count);
    let packet_count = bins.len();
    let runs = (0..iterations).map(|_| bins.clone()).collect::<Vec<_>>();
    let start = Instant::now();
    let mut decoded = 0usize;
    for run in runs {
        decoded += mettle_decode_once(params, source_symbol_bytes, source_count, run);
    }
    let elapsed = start.elapsed();

    assert_eq!(decoded, iterations * source_count);
    (
        packet_count,
        elapsed.as_nanos() / (iterations as u128 * packet_count as u128),
    )
}

fn benchmark_raptorq_ns_per_packet(iterations: usize, source_count: usize) -> u128 {
    let (oti, block_length, packets) = raptorq_repair_only_fixture(source_count);
    let runs = (0..iterations).map(|_| packets.clone()).collect::<Vec<_>>();
    let start = Instant::now();
    let mut decoded = 0usize;
    for run in runs {
        decoded += raptorq_decode_once(&oti, block_length, run);
    }
    let elapsed = start.elapsed();

    assert_eq!(decoded, iterations * source_count);
    elapsed.as_nanos() / (iterations as u128 * source_count as u128)
}

fn env_or_default_usize(name: &str, default: usize) -> usize {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(default)
}

#[test]
fn paper_decode_speed_harness_builds() {
    let (params, symbol_bytes, bins) = mettle_streaming_fixture(2048);
    assert!(!bins.is_empty());
    assert!(mettle_decode_once(params, symbol_bytes, 2048, bins) >= 2048);

    let (oti, block_length, packets) = raptorq_repair_only_fixture(127);
    assert_eq!(raptorq_decode_once(&oti, block_length, packets), 127);
}

#[test]
#[ignore = "manual paper-style decode-speed reproduction"]
fn report_paper_decode_speed() {
    let iterations = env_or_default_usize("METTLE_PAPER_SPEED_ITERATIONS", 10);
    let mettle_source_count =
        env_or_default_usize("METTLE_PAPER_SPEED_METTLE_SOURCE_COUNT", PAPER_SPEED_METTLE_SOURCE_COUNT);
    let k_filter = std::env::var("METTLE_PAPER_SPEED_FILTER").ok();

    let (mettle_packet_count, mettle_ns_per_packet) =
        benchmark_mettle_ns_per_packet(iterations, mettle_source_count);
    let mettle_us_per_packet = mettle_ns_per_packet as f64 / 1_000.0;
    eprintln!(
        "mettle source_count={} packet_count={} ns_per_packet={} us_per_packet={:.3}",
        mettle_source_count,
        mettle_packet_count,
        mettle_ns_per_packet,
        mettle_us_per_packet,
    );

    for source_count in TABLE_IV_LATENCY_MATCHED_KS {
        if let Some(filter) = &k_filter
            && source_count.to_string() != *filter
        {
            continue;
        }
        let raptorq_ns_per_packet = benchmark_raptorq_ns_per_packet(iterations, source_count);
        let raptorq_us_per_packet = raptorq_ns_per_packet as f64 / 1_000.0;
        eprintln!(
            "latency_matched_k={} raptorq_ns_per_packet={} raptorq_us_per_packet={:.3} ratio={:.2}",
            source_count,
            raptorq_ns_per_packet,
            raptorq_us_per_packet,
            raptorq_us_per_packet / mettle_us_per_packet,
        );
    }

    for (source_count, paper_raptorq_us_per_packet) in TABLE_V_RAPTORQ_ROWS {
        if let Some(filter) = &k_filter
            && source_count.to_string() != *filter
        {
            continue;
        }
        let raptorq_ns_per_packet = benchmark_raptorq_ns_per_packet(iterations, source_count);
        let raptorq_us_per_packet = raptorq_ns_per_packet as f64 / 1_000.0;
        eprintln!(
            "table_v_k={} raptorq_ns_per_packet={} raptorq_us_per_packet={:.3} paper_raptorq_us_per_packet={} ratio={:.2} paper_ratio={:.2}",
            source_count,
            raptorq_ns_per_packet,
            raptorq_us_per_packet,
            paper_raptorq_us_per_packet,
            raptorq_us_per_packet / mettle_us_per_packet,
            paper_raptorq_us_per_packet as f64 / mettle_us_per_packet,
        );
    }
}
