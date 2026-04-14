use raptorq::{EncodingPacket, ObjectTransmissionInformation, SourceBlockDecoder, SourceBlockEncoder};

const TABLE_IV_SYMBOL_SIZE: usize = 1500;
const TARGET_FAILURE_RATE: f64 = 1e-3;

#[derive(Clone, Copy)]
struct Rational {
    numerator: u32,
    denominator: u32,
}

impl Rational {
    const fn new(numerator: u32, denominator: u32) -> Self {
        Self {
            numerator,
            denominator,
        }
    }

    fn to_f64(self) -> f64 {
        f64::from(self.numerator) / f64::from(self.denominator)
    }
}

#[derive(Clone, Copy)]
enum Channel {
    Bec {
        erasure_probability: Rational,
    },
    Ge {
        p_good_to_bad: Rational,
        p_bad_to_good: Rational,
        epsilon_good: Rational,
        epsilon_bad: Rational,
    },
}

#[derive(Clone, Copy)]
struct TableIvRow {
    name: &'static str,
    channel: Channel,
    k: usize,
    overhead_ratio: Rational,
}

const TABLE_IV_ROWS: [TableIvRow; 10] = [
    TableIvRow {
        name: "BEC(0.01)",
        channel: Channel::Bec {
            erasure_probability: Rational::new(1, 100),
        },
        k: 114,
        overhead_ratio: Rational::new(614, 10_000),
    },
    TableIvRow {
        name: "BEC(0.02)",
        channel: Channel::Bec {
            erasure_probability: Rational::new(2, 100),
        },
        k: 168,
        overhead_ratio: Rational::new(714, 10_000),
    },
    TableIvRow {
        name: "BEC(0.03)",
        channel: Channel::Bec {
            erasure_probability: Rational::new(3, 100),
        },
        k: 236,
        overhead_ratio: Rational::new(763, 10_000),
    },
    TableIvRow {
        name: "BEC(0.08)",
        channel: Channel::Bec {
            erasure_probability: Rational::new(8, 100),
        },
        k: 269,
        overhead_ratio: Rational::new(1560, 10_000),
    },
    TableIvRow {
        name: "BEC(0.10)",
        channel: Channel::Bec {
            erasure_probability: Rational::new(10, 100),
        },
        k: 405,
        overhead_ratio: Rational::new(1500, 10_000),
    },
    TableIvRow {
        name: "VoIP",
        channel: Channel::Ge {
            p_good_to_bad: Rational::new(5, 10_000),
            p_bad_to_good: Rational::new(1, 5),
            epsilon_good: Rational::new(1, 100),
            epsilon_bad: Rational::new(1, 1),
        },
        k: 84,
        overhead_ratio: Rational::new(2380, 10_000),
    },
    TableIvRow {
        name: "WiMAX",
        channel: Channel::Ge {
            p_good_to_bad: Rational::new(4, 100),
            p_bad_to_good: Rational::new(5, 100),
            epsilon_good: Rational::new(1, 100),
            epsilon_bad: Rational::new(2, 100),
        },
        k: 149,
        overhead_ratio: Rational::new(604, 10_000),
    },
    TableIvRow {
        name: "Video-conf-light",
        channel: Channel::Ge {
            p_good_to_bad: Rational::new(5, 100),
            p_bad_to_good: Rational::new(75, 100),
            epsilon_good: Rational::new(1, 100),
            epsilon_bad: Rational::new(1, 10),
        },
        k: 114,
        overhead_ratio: Rational::new(702, 10_000),
    },
    TableIvRow {
        name: "Video-conf-heavy",
        channel: Channel::Ge {
            p_good_to_bad: Rational::new(5, 100),
            p_bad_to_good: Rational::new(75, 100),
            epsilon_good: Rational::new(5, 100),
            epsilon_bad: Rational::new(1, 2),
        },
        k: 257,
        overhead_ratio: Rational::new(1556, 10_000),
    },
    TableIvRow {
        name: "Long-fade",
        channel: Channel::Ge {
            p_good_to_bad: Rational::new(1, 1000),
            p_bad_to_good: Rational::new(1, 100),
            epsilon_good: Rational::new(1, 100),
            epsilon_bad: Rational::new(1, 10),
        },
        k: 101,
        overhead_ratio: Rational::new(1584, 10_000),
    },
];

fn table_iv_flat_data(source_count: usize) -> Vec<u8> {
    (0..source_count)
        .flat_map(|source_id| {
            let mut payload = vec![0; TABLE_IV_SYMBOL_SIZE];
            payload[0] = source_id as u8;
            payload
        })
        .collect()
}

fn total_packet_count(source_count: usize, overhead_ratio: Rational) -> usize {
    div_ceil(
        source_count * (overhead_ratio.denominator as usize + overhead_ratio.numerator as usize),
        overhead_ratio.denominator as usize,
    )
}

fn raptorq_trial_succeeds(row: TableIvRow, seed: u64) -> bool {
    let total_packets = total_packet_count(row.k, row.overhead_ratio);
    let repair_packets = total_packets.saturating_sub(row.k);
    let flat_data = table_iv_flat_data(row.k);
    let oti = ObjectTransmissionInformation::new(
        flat_data.len() as u64,
        TABLE_IV_SYMBOL_SIZE as u16,
        1,
        1,
        1,
    );
    let encoder = SourceBlockEncoder::new(0, &oti, &flat_data);
    let mut delivered_packets = Vec::<EncodingPacket>::new();
    let mut channel_state = ChannelState::new(row.channel, seed ^ 0xC0DE_CAFE_F00D_BAAD);

    for packet in encoder.source_packets() {
        if channel_state.delivers_next_packet() {
            delivered_packets.push(packet);
        }
    }
    for packet in encoder.repair_packets(0, repair_packets as u32) {
        if channel_state.delivers_next_packet() {
            delivered_packets.push(packet);
        }
    }

    SourceBlockDecoder::new(0, &oti, flat_data.len() as u64)
        .decode(delivered_packets)
        .is_some_and(|decoded| decoded == flat_data)
}

fn estimated_failure_rate(row: TableIvRow, trials: usize) -> f64 {
    let failures = (0..trials)
        .filter(|&trial| !raptorq_trial_succeeds(row, trial as u64 + 1))
        .count();

    failures as f64 / trials as f64
}

struct ChannelState {
    channel: Channel,
    rng: SplitMix64,
    in_bad_state: bool,
}

impl ChannelState {
    fn new(channel: Channel, seed: u64) -> Self {
        let mut rng = SplitMix64::new(seed);
        let in_bad_state = match channel {
            Channel::Bec { .. } => false,
            Channel::Ge {
                p_good_to_bad,
                p_bad_to_good,
                ..
            } => {
                let total = p_good_to_bad.to_f64() + p_bad_to_good.to_f64();
                rng.next_unit_f64() < p_good_to_bad.to_f64() / total
            }
        };

        Self {
            channel,
            rng,
            in_bad_state,
        }
    }

    fn delivers_next_packet(&mut self) -> bool {
        match self.channel {
            Channel::Bec {
                erasure_probability,
            } => self.rng.next_unit_f64() >= erasure_probability.to_f64(),
            Channel::Ge {
                p_good_to_bad,
                p_bad_to_good,
                epsilon_good,
                epsilon_bad,
            } => {
                let erasure_probability = if self.in_bad_state {
                    epsilon_bad.to_f64()
                } else {
                    epsilon_good.to_f64()
                };
                let delivered = self.rng.next_unit_f64() >= erasure_probability;
                let transition_probability = if self.in_bad_state {
                    p_bad_to_good.to_f64()
                } else {
                    p_good_to_bad.to_f64()
                };
                if self.rng.next_unit_f64() < transition_probability {
                    self.in_bad_state = !self.in_bad_state;
                }
                delivered
            }
        }
    }
}

struct SplitMix64 {
    state: u64,
}

impl SplitMix64 {
    fn new(seed: u64) -> Self {
        Self { state: seed }
    }

    fn next_u64(&mut self) -> u64 {
        self.state = self.state.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut value = self.state;
        value = (value ^ (value >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        value = (value ^ (value >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        value ^ (value >> 31)
    }

    fn next_unit_f64(&mut self) -> f64 {
        let mantissa = self.next_u64() >> 11;
        mantissa as f64 / ((1u64 << 53) as f64)
    }
}

const fn div_ceil(lhs: usize, rhs: usize) -> usize {
    lhs / rhs + ((lhs % rhs) != 0) as usize
}

#[test]
fn table_iv_raptorq_harness_decodes_a_small_bec_case() {
    let row = TABLE_IV_ROWS[0];

    assert!(raptorq_trial_succeeds(row, 1));
}

#[test]
#[ignore = "manual Table IV RaptorQ reproduction"]
fn report_table_iv_raptorq_failure_rates() {
    let trials = std::env::var("METTLE_TABLE_IV_TRIALS")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(4096);
    let name_filter = std::env::var("METTLE_TABLE_IV_FILTER").ok();

    for row in TABLE_IV_ROWS {
        if let Some(filter) = &name_filter {
            if !row.name.contains(filter) {
                continue;
            }
        }
        let failure_rate = estimated_failure_rate(row, trials);
        eprintln!(
            "channel={} k={} overhead={:.4}% estimated_failure_rate={:.6} target={:.6}",
            row.name,
            row.k,
            row.overhead_ratio.to_f64() * 100.0,
            failure_rate,
            TARGET_FAILURE_RATE,
        );
    }
}
