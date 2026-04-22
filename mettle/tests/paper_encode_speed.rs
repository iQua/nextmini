use std::num::NonZeroUsize;
use std::time::Instant;

use mettle::test_support::Encoder as TestEncoder;
use mettle::{MettleParams, OverheadRatio};
use raptorq::{ObjectTransmissionInformation, SourceBlockEncoder};

const PAPER_SPEED_SYMBOL_SIZE: usize = 1500;
const PAPER_SPEED_METTLE_STREAM_SOURCE_COUNT: usize = 100_000;
const PAPER_SPEED_SEED: u64 = 0;

const ALL_KS: [usize; 15] = [
    84, 101, 114, 127, 149, 168, 236, 257, 269, 405, 511, 1002, 2040, 4069, 8194,
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
    (0..source_count)
        .flat_map(|source_id| {
            let mut payload = vec![0; PAPER_SPEED_SYMBOL_SIZE];
            payload[0] = source_id as u8;
            payload
        })
        .collect()
}

fn mettle_encode_ns_per_packet(iterations: usize, source_count: usize) -> (usize, u128) {
    let params = MettleParams::new(OverheadRatio::new(1, 20).expect("valid overhead"));
    let source_symbol_bytes = NonZeroUsize::new(PAPER_SPEED_SYMBOL_SIZE).expect("non-zero");
    let sources = benchmark_sources(source_count);
    let mut total_packets = 0usize;

    let start = Instant::now();
    for _ in 0..iterations {
        let mut encoder = TestEncoder::new_terminated(
            params,
            source_symbol_bytes,
            PAPER_SPEED_SEED,
            source_count as u64,
        );
        for source in &sources {
            total_packets += encoder.push_source(source).len();
        }
        total_packets += encoder.finish().len();
    }
    let elapsed = start.elapsed();

    assert!(total_packets >= iterations * source_count);
    let packets_per_iter = total_packets / iterations;
    let ns_per_packet = elapsed.as_nanos() / total_packets as u128;
    (packets_per_iter, ns_per_packet)
}

fn raptorq_encode_ns_per_packet(iterations: usize, k: usize) -> (usize, u128) {
    let transfer_length = (k * PAPER_SPEED_SYMBOL_SIZE) as u64;
    let oti = ObjectTransmissionInformation::new(
        transfer_length,
        PAPER_SPEED_SYMBOL_SIZE as u16,
        1,
        1,
        1,
    );
    let flat_data = benchmark_flat_data(k);
    let repair_count = k.div_ceil(20);
    let mut total_packets = 0usize;
    let mut total_ns = 0u128;

    for _ in 0..iterations {
        let start = Instant::now();
        let encoder = SourceBlockEncoder::new(0, &oti, &flat_data);
        let source_packets = encoder.source_packets();
        let repair_packets = encoder.repair_packets(0, repair_count as u32);
        total_ns += start.elapsed().as_nanos();
        total_packets += source_packets.len() + repair_packets.len();
    }

    assert!(total_packets >= iterations * (k + repair_count));
    let packets_per_iter = total_packets / iterations;
    let ns_per_packet = total_ns / total_packets as u128;
    (packets_per_iter, ns_per_packet)
}

#[test]
fn paper_encode_speed_harness_builds() {
    let (mettle_packets, _) = mettle_encode_ns_per_packet(1, 2048);
    assert!(mettle_packets >= 2048);

    let (raptorq_packets, _) = raptorq_encode_ns_per_packet(1, 127);
    assert!(raptorq_packets >= 127);
}

#[test]
#[ignore = "manual paper-style encode-speed reproduction"]
fn report_paper_encode_speed() {
    let iterations = std::env::var("METTLE_PAPER_SPEED_ITERATIONS")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(10);
    let mettle_source_count = std::env::var("METTLE_PAPER_SPEED_METTLE_SOURCE_COUNT")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(PAPER_SPEED_METTLE_STREAM_SOURCE_COUNT);
    let k_filter = std::env::var("METTLE_PAPER_SPEED_FILTER").ok();

    let (mettle_packet_count, mettle_ns_per_packet) =
        mettle_encode_ns_per_packet(iterations, mettle_source_count);
    let mettle_us_per_packet = mettle_ns_per_packet as f64 / 1_000.0;
    eprintln!(
        "mettle_encode source_count={} packet_count={} ns_per_packet={} us_per_packet={:.3}",
        mettle_source_count, mettle_packet_count, mettle_ns_per_packet, mettle_us_per_packet,
    );

    for k in ALL_KS {
        if let Some(filter) = &k_filter
            && k.to_string() != *filter
        {
            continue;
        }
        let (raptorq_packet_count, raptorq_ns_per_packet) =
            raptorq_encode_ns_per_packet(iterations, k);
        let raptorq_us_per_packet = raptorq_ns_per_packet as f64 / 1_000.0;
        eprintln!(
            "raptorq_encode k={} packet_count={} ns_per_packet={} us_per_packet={:.3} ratio={:.2}",
            k,
            raptorq_packet_count,
            raptorq_ns_per_packet,
            raptorq_us_per_packet,
            raptorq_us_per_packet / mettle_us_per_packet,
        );
    }
}
