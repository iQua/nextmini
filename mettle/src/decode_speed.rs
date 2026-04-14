use std::num::NonZeroUsize;
use std::time::Instant;

use raptorq::{EncodingPacket, ObjectTransmissionInformation, SourceBlockDecoder, SourceBlockEncoder};

use crate::decoder::MettleDecoder;
use crate::encoder::{MettleBin, MettleEncoder};
use crate::{MettleParams, OverheadRatio};

// Table V in the paper reports RaptorQ decode cost at representative
// latency-matched block sizes including k = 127.
const BENCH_SOURCE_COUNT: usize = 127;
const BENCH_SYMBOL_SIZE: usize = 1500;

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
    let mut emitted_bins = Vec::new();

    for source in benchmark_sources() {
        emitted_bins.extend(encoder.push_source(&source));
    }
    emitted_bins.extend(encoder.finish());

    let mut decoder = MettleDecoder::new(params, source_symbol_bytes, 0);
    let mut decoded_sources = 0usize;
    let mut completion_prefix = Vec::new();

    for bin in emitted_bins {
        decoded_sources += decoder.push_bin(bin.clone()).len();
        completion_prefix.push(bin);
        if decoded_sources == BENCH_SOURCE_COUNT {
            return (params, source_symbol_bytes, completion_prefix);
        }
    }

    panic!("METTLE fixture did not complete decode");
}

fn raptorq_decode_fixture(
    target_packet_count: usize,
) -> (ObjectTransmissionInformation, u64, Vec<EncodingPacket>, Vec<u8>) {
    let oti = ObjectTransmissionInformation::new(
        (BENCH_SOURCE_COUNT * BENCH_SYMBOL_SIZE) as u64,
        BENCH_SYMBOL_SIZE as u16,
        1,
        1,
        1,
    );
    let flat_data = benchmark_flat_data();
    let encoder = SourceBlockEncoder::new(0, &oti, &flat_data);
    let source_packet_count = BENCH_SOURCE_COUNT.min(target_packet_count);
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
    let (_, _, mettle_bins) = mettle_decode_fixture();
    let (oti, block_length, packets, flat_data) = raptorq_decode_fixture(mettle_bins.len());
    let packet_count = packets.len();
    let decoded = SourceBlockDecoder::new(0, &oti, block_length)
        .decode(packets)
        .expect("raptorq fixture should decode");

    assert!(!mettle_bins.is_empty());
    assert!(mettle_bins.len() >= BENCH_SOURCE_COUNT);
    assert_eq!(packet_count, mettle_bins.len());
    assert_eq!(decoded, flat_data);
    assert_eq!(decoded.len(), BENCH_SOURCE_COUNT * BENCH_SYMBOL_SIZE);
}

fn mettle_decode_once(
    params: MettleParams,
    source_symbol_bytes: NonZeroUsize,
    bins: Vec<MettleBin>,
) -> usize {
    let mut decoder = MettleDecoder::new(params, source_symbol_bytes, 0);
    let mut decoded = Vec::new();

    for bin in bins {
        decoded.extend(decoder.push_bin(bin));
    }

    decoded.len()
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

#[test]
#[ignore = "manual decode-speed checkpoint"]
fn report_decode_speed_ratio() {
    const ITERATIONS: usize = 5;
    let (mettle_params, mettle_symbol_bytes, mettle_bins) = mettle_decode_fixture();
    let mettle_packet_count = mettle_bins.len();
    let (raptorq_oti, raptorq_block_length, raptorq_packets, _) =
        raptorq_decode_fixture(mettle_packet_count);
    let mettle_runs = (0..ITERATIONS)
        .map(|_| mettle_bins.clone())
        .collect::<Vec<_>>();
    let raptorq_runs = (0..ITERATIONS)
        .map(|_| raptorq_packets.clone())
        .collect::<Vec<_>>();

    let mettle_start = Instant::now();
    let mut mettle_decoded = 0;
    for bins in mettle_runs {
        mettle_decoded += mettle_decode_once(mettle_params, mettle_symbol_bytes, bins);
    }
    let mettle_elapsed = mettle_start.elapsed();

    let raptorq_start = Instant::now();
    let mut raptorq_decoded = 0;
    for packets in raptorq_runs {
        raptorq_decoded += raptorq_decode_once(&raptorq_oti, raptorq_block_length, packets);
    }
    let raptorq_elapsed = raptorq_start.elapsed();

    assert_eq!(mettle_decoded, ITERATIONS * BENCH_SOURCE_COUNT);
    assert_eq!(raptorq_decoded, ITERATIONS * BENCH_SOURCE_COUNT);

    let total_packets = ITERATIONS as u128 * mettle_packet_count as u128;
    let mettle_ns_per_packet = mettle_elapsed.as_nanos() / total_packets;
    let raptorq_ns_per_packet = raptorq_elapsed.as_nanos() / total_packets;
    let ratio = raptorq_ns_per_packet as f64 / mettle_ns_per_packet as f64;

    eprintln!(
        "packet_count={mettle_packet_count} mettle_ns_per_packet={mettle_ns_per_packet} raptorq_ns_per_packet={raptorq_ns_per_packet} ratio={ratio:.2}"
    );
}
