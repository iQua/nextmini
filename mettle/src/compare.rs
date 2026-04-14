use std::num::NonZeroUsize;

use raptorq::{EncodingPacket, ObjectTransmissionInformation, SourceBlockDecoder, SourceBlockEncoder};

use crate::decoder::{DecodedSource, MettleDecoder};
use crate::encoder::MettleEncoder;
use crate::{MettleParams, OverheadRatio};

const COMPARE_SYMBOL_SIZE: usize = 1500;
const COMPARE_SOURCE_COUNT: usize = 127;
const MAX_COMPARE_PACKETS_MULTIPLIER: usize = 4;

#[derive(Clone, Copy)]
enum CompareScenario {
    NoLoss,
    Bec { erasure_numerator: u64, erasure_denominator: u64 },
}

impl CompareScenario {
    fn name(self) -> &'static str {
        match self {
            Self::NoLoss => "no_loss",
            Self::Bec {
                erasure_numerator: 1,
                erasure_denominator: 100,
            } => "bec_0_01",
            Self::Bec { .. } => "bec",
        }
    }

    fn keeps_packet(self, packet_ordinal: u64) -> bool {
        match self {
            Self::NoLoss => true,
            Self::Bec {
                erasure_numerator,
                erasure_denominator,
            } => pseudo_uniform(packet_ordinal) % erasure_denominator >= erasure_numerator,
        }
    }
}

#[derive(Debug, PartialEq, Eq)]
struct CompareOutcome {
    sent_packets: usize,
    delivered_packets: usize,
    source_count: usize,
}

impl CompareOutcome {
    fn delivered_overhead_ratio(&self) -> f64 {
        self.delivered_packets as f64 / self.source_count as f64 - 1.0
    }
}

fn compare_sources(source_count: usize) -> Vec<Vec<u8>> {
    (0..source_count)
        .map(|source_id| {
            let mut payload = vec![0; COMPARE_SYMBOL_SIZE];
            payload[0] = source_id as u8;
            payload
        })
        .collect()
}

fn flat_compare_sources(source_count: usize) -> Vec<u8> {
    compare_sources(source_count).into_iter().flatten().collect()
}

fn expected_mettle_decode(source_count: usize) -> Vec<(u64, Vec<u8>)> {
    compare_sources(source_count)
        .into_iter()
        .enumerate()
        .map(|(source_id, payload)| (source_id as u64, payload))
        .collect()
}

fn mettle_compare_outcome(source_count: usize, scenario: CompareScenario) -> CompareOutcome {
    let params = MettleParams::new(OverheadRatio::new(1, 20).expect("valid overhead"));
    let source_symbol_bytes = NonZeroUsize::new(COMPARE_SYMBOL_SIZE).expect("non-zero");
    let mut encoder = MettleEncoder::new_terminated(params, source_symbol_bytes, 0, source_count as u64);
    let mut emitted_bins = Vec::new();

    for source in compare_sources(source_count) {
        emitted_bins.extend(encoder.push_source(&source));
    }
    emitted_bins.extend(encoder.finish());

    let mut decoder = MettleDecoder::new_terminated(params, source_symbol_bytes, 0, source_count as u64);
    let mut decoded_sources = Vec::new();
    let mut sent_packets = 0usize;
    let mut delivered_packets = 0usize;
    let expected = expected_mettle_decode(source_count);

    for (packet_ordinal, bin) in emitted_bins.into_iter().enumerate() {
        sent_packets += 1;
        if !scenario.keeps_packet(packet_ordinal as u64) {
            continue;
        }
        delivered_packets += 1;
        decoded_sources.extend(
            decoder
                .push_bin(bin)
                .into_iter()
                .map(DecodedSource::into_parts),
        );
        if decoded_sources.len() == source_count {
            assert_eq!(decoded_sources, expected);
            return CompareOutcome {
                sent_packets,
                delivered_packets,
                source_count,
            };
        }
    }

    panic!("METTLE compare fixture did not complete decode");
}

fn raptorq_compare_outcome(source_count: usize, scenario: CompareScenario) -> CompareOutcome {
    let oti = ObjectTransmissionInformation::new(
        (source_count * COMPARE_SYMBOL_SIZE) as u64,
        COMPARE_SYMBOL_SIZE as u16,
        1,
        1,
        1,
    );
    let flat_data = flat_compare_sources(source_count);
    let encoder = SourceBlockEncoder::new(0, &oti, &flat_data);
    let mut delivered_packets = Vec::<EncodingPacket>::new();
    let mut sent_packets = 0usize;
    let mut delivered_count = 0usize;
    let mut next_repair_id = 0u32;
    let max_sent_packets = source_count * MAX_COMPARE_PACKETS_MULTIPLIER;

    for packet in encoder.source_packets() {
        sent_packets += 1;
        if scenario.keeps_packet((sent_packets - 1) as u64) {
            delivered_count += 1;
            delivered_packets.push(packet);
        }
    }

    loop {
        if delivered_count >= source_count {
            let decoded = SourceBlockDecoder::new(0, &oti, flat_data.len() as u64)
                .decode(delivered_packets.clone());
            if let Some(decoded) = decoded {
                assert_eq!(decoded, flat_data);
                return CompareOutcome {
                    sent_packets,
                    delivered_packets: delivered_count,
                    source_count,
                };
            }
        }

        assert!(sent_packets < max_sent_packets, "RaptorQ compare fixture exceeded the packet budget");
        let packet = encoder
            .repair_packets(next_repair_id, 1)
            .into_iter()
            .next()
            .expect("single repair packet");
        next_repair_id += 1;
        sent_packets += 1;
        if scenario.keeps_packet((sent_packets - 1) as u64) {
            delivered_count += 1;
            delivered_packets.push(packet);
        }
    }
}

fn pseudo_uniform(packet_ordinal: u64) -> u64 {
    let mut value = packet_ordinal.wrapping_add(0x9E37_79B9_7F4A_7C15);
    value = (value ^ (value >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    value = (value ^ (value >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    value ^ (value >> 31)
}

#[test]
fn fair_compare_harness_completes_without_loss() {
    let mettle = mettle_compare_outcome(COMPARE_SOURCE_COUNT, CompareScenario::NoLoss);
    let raptorq = raptorq_compare_outcome(COMPARE_SOURCE_COUNT, CompareScenario::NoLoss);

    assert_eq!(mettle.source_count, COMPARE_SOURCE_COUNT);
    assert_eq!(raptorq.source_count, COMPARE_SOURCE_COUNT);
    assert!(mettle.delivered_packets >= COMPARE_SOURCE_COUNT);
    assert_eq!(raptorq.delivered_packets, COMPARE_SOURCE_COUNT);
}

#[test]
fn fair_compare_harness_completes_under_light_loss() {
    let scenario = CompareScenario::Bec {
        erasure_numerator: 1,
        erasure_denominator: 100,
    };
    let mettle = mettle_compare_outcome(COMPARE_SOURCE_COUNT, scenario);
    let raptorq = raptorq_compare_outcome(COMPARE_SOURCE_COUNT, scenario);

    assert!(mettle.sent_packets >= mettle.delivered_packets);
    assert!(raptorq.sent_packets >= raptorq.delivered_packets);
    assert!(mettle.delivered_packets >= COMPARE_SOURCE_COUNT);
    assert!(raptorq.delivered_packets >= COMPARE_SOURCE_COUNT);
}

#[test]
#[ignore = "manual fair-compare checkpoint"]
fn report_fair_compare_harness() {
    for scenario in [
        CompareScenario::NoLoss,
        CompareScenario::Bec {
            erasure_numerator: 1,
            erasure_denominator: 100,
        },
    ] {
        let mettle = mettle_compare_outcome(COMPARE_SOURCE_COUNT, scenario);
        let raptorq = raptorq_compare_outcome(COMPARE_SOURCE_COUNT, scenario);
        eprintln!(
            "scenario={} mettle_sent={} mettle_delivered={} mettle_overhead={:.4} raptorq_sent={} raptorq_delivered={} raptorq_overhead={:.4}",
            scenario.name(),
            mettle.sent_packets,
            mettle.delivered_packets,
            mettle.delivered_overhead_ratio(),
            raptorq.sent_packets,
            raptorq.delivered_packets,
            raptorq.delivered_overhead_ratio(),
        );
    }
}
