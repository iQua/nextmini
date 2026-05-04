use std::collections::{HashMap, HashSet, VecDeque};
use std::num::NonZeroUsize;
use std::time::{Duration, Instant};

use mettle::test_support::{
    Decoder as TestDecoder, Encoder as TestEncoder, edge_bin_ids_with_terminal_source_count,
};
use mettle::{MettleParams, OverheadRatio};
use raptorq::{
    EncodingPacket, ObjectTransmissionInformation, SourceBlockDecoder, SourceBlockEncoder,
};

const DEFAULT_SEED: u64 = 0;
const DEFAULT_SYMBOL_SIZE: usize = 1500;
const DEFAULT_METTLE_OVERHEAD_NUMERATOR: u32 = 1;
const DEFAULT_METTLE_OVERHEAD_DENOMINATOR: u32 = 20;
const DEFAULT_DECODE_LOSS_RATES: &[f64] = &[
    0.0, 0.0025, 0.005, 0.0075, 0.01, 0.015, 0.02, 0.03, 0.04, 0.05, 0.075, 0.10,
];
const DEFAULT_EFFICIENCY_LOSS_RATES: &[f64] = &[
    0.005, 0.01, 0.015, 0.02, 0.03, 0.04, 0.05, 0.075, 0.10, 0.125, 0.15, 0.20,
];
const DEFAULT_EFFICIENCY_OVERHEADS: &[f64] = &[
    0.01, 0.02, 0.03, 0.04, 0.05, 0.06, 0.08, 0.10, 0.12, 0.15, 0.18, 0.20, 0.25, 0.30, 0.40,
];

#[derive(Clone, Copy)]
enum Codec {
    Raptorq,
    Mettle,
}

#[derive(Clone, Copy)]
enum LossDecodeBudget {
    PaperBec,
    FixedTargetRxOverhead(f64),
}

impl LossDecodeBudget {
    fn label(self) -> &'static str {
        match self {
            Self::PaperBec => "paper_bec",
            Self::FixedTargetRxOverhead(_) => "fixed_target_rx_overhead",
        }
    }

    fn base_target_rx_overhead(self, codec: Codec, loss_rate: f64) -> Option<f64> {
        match self {
            Self::PaperBec => paper_ceiling_bec_overheads(loss_rate).map(|overheads| match codec {
                Codec::Raptorq => overheads.raptorq,
                Codec::Mettle => overheads.mettle,
            }),
            Self::FixedTargetRxOverhead(overhead) => Some(overhead),
        }
    }
}

#[derive(Clone, Copy)]
struct PaperBecOverheads {
    mettle: f64,
    raptorq: f64,
}

#[derive(Clone, Copy)]
struct DecodeConfig {
    symbol_size: usize,
    iterations: usize,
    mettle_overhead: OverheadRatio,
}

struct DecodeMetrics {
    received_packets: usize,
    elapsed: Duration,
}

#[derive(Clone, Copy)]
struct EncodeMetrics {
    configured_tx_packets: usize,
    tx_packets: usize,
    elapsed: Duration,
}

#[derive(Clone, Copy)]
struct LossDecodeConfig {
    symbol_size: usize,
    trials: usize,
    budget: LossDecodeBudget,
    overhead_margin: f64,
    mettle_local_residual_source_limit: usize,
}

#[derive(Clone, Copy)]
struct LossDecodeMetrics {
    trials: usize,
    paper_successes: usize,
    strict_successes: usize,
    isolated_error_floor_events: usize,
    local_residual_events: usize,
    stall_failures: usize,
    mixed_failures: usize,
    non_isolated_residual_sources_total: usize,
    non_isolated_residual_sources_max: usize,
    isolated_sources_total: usize,
    isolated_sources_max: usize,
    configured_tx_packets: usize,
    tx_packets: usize,
    total_received_packets: usize,
    successful_received_packets: usize,
    successful_elapsed: Duration,
}

impl LossDecodeMetrics {
    fn new(trials: usize, configured_tx_packets: usize, tx_packets: usize) -> Self {
        Self {
            trials,
            paper_successes: 0,
            strict_successes: 0,
            isolated_error_floor_events: 0,
            local_residual_events: 0,
            stall_failures: 0,
            mixed_failures: 0,
            non_isolated_residual_sources_total: 0,
            non_isolated_residual_sources_max: 0,
            isolated_sources_total: 0,
            isolated_sources_max: 0,
            configured_tx_packets,
            tx_packets,
            total_received_packets: 0,
            successful_received_packets: 0,
            successful_elapsed: Duration::ZERO,
        }
    }

    fn record_paper_success(&mut self, received_packets: usize, elapsed: Duration) {
        self.paper_successes += 1;
        self.successful_received_packets += received_packets;
        self.successful_elapsed += elapsed;
    }

    fn record_strict_success(&mut self) {
        self.strict_successes += 1;
    }

    fn record_mettle_outcome(
        &mut self,
        outcome: MettlePaperOutcome,
        local_residual_source_limit: usize,
        received_packets: usize,
        elapsed: Duration,
        strict_success: bool,
    ) {
        self.non_isolated_residual_sources_total += outcome.non_isolated_residual_sources;
        self.non_isolated_residual_sources_max = self
            .non_isolated_residual_sources_max
            .max(outcome.non_isolated_residual_sources);
        self.isolated_sources_total += outcome.isolated_sources;
        self.isolated_sources_max = self.isolated_sources_max.max(outcome.isolated_sources);

        if strict_success {
            self.record_strict_success();
        }

        if outcome.is_paper_success(local_residual_source_limit) {
            self.record_paper_success(received_packets, elapsed);
        }

        match outcome.classify(local_residual_source_limit) {
            MettlePaperOutcomeClass::FullDecode => {}
            MettlePaperOutcomeClass::IsolatedErrorFloor => {
                self.isolated_error_floor_events += 1;
            }
            MettlePaperOutcomeClass::LocalResidual => {
                self.local_residual_events += 1;
            }
            MettlePaperOutcomeClass::StallFailure => {
                self.stall_failures += 1;
            }
            MettlePaperOutcomeClass::MixedFailure => {
                self.mixed_failures += 1;
            }
        }
    }
}

#[derive(Clone, Copy)]
struct MettlePaperOutcome {
    non_isolated_residual_sources: usize,
    isolated_sources: usize,
}

impl MettlePaperOutcome {
    fn classify(self, local_residual_source_limit: usize) -> MettlePaperOutcomeClass {
        if self.non_isolated_residual_sources == 0 {
            if self.isolated_sources == 0 {
                MettlePaperOutcomeClass::FullDecode
            } else {
                MettlePaperOutcomeClass::IsolatedErrorFloor
            }
        } else if self.non_isolated_residual_sources <= local_residual_source_limit {
            MettlePaperOutcomeClass::LocalResidual
        } else if self.isolated_sources == 0 {
            MettlePaperOutcomeClass::StallFailure
        } else {
            MettlePaperOutcomeClass::MixedFailure
        }
    }

    fn is_paper_success(self, local_residual_source_limit: usize) -> bool {
        matches!(
            self.classify(local_residual_source_limit),
            MettlePaperOutcomeClass::FullDecode
                | MettlePaperOutcomeClass::IsolatedErrorFloor
                | MettlePaperOutcomeClass::LocalResidual
        )
    }
}

#[derive(Clone, Copy)]
enum MettlePaperOutcomeClass {
    FullDecode,
    IsolatedErrorFloor,
    LocalResidual,
    StallFailure,
    MixedFailure,
}

#[derive(Clone, Copy)]
struct EfficiencyConfig {
    symbol_size: usize,
    trials: usize,
}

#[derive(Clone, Copy)]
struct EfficiencyMetrics {
    trials: usize,
    successes: usize,
    avg_tx_packets: f64,
    avg_rx_packets: f64,
}

#[derive(Clone, Copy)]
struct Prng(u64);

impl Prng {
    fn new(seed: u64) -> Self {
        Self(seed.max(1))
    }

    fn next_u64(&mut self) -> u64 {
        let mut value = self.0;
        value ^= value << 13;
        value ^= value >> 7;
        value ^= value << 17;
        self.0 = value;
        value
    }

    fn keep_packet(&mut self, loss_rate: f64) -> bool {
        if loss_rate <= 0.0 {
            return true;
        }
        if loss_rate >= 1.0 {
            return false;
        }
        let threshold = (loss_rate * 1_000_000.0).round() as u64;
        self.next_u64() % 1_000_000 >= threshold
    }
}

fn env_usize(name: &str, default: usize) -> usize {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}

fn env_usize_any(names: &[&str], default: usize) -> usize {
    names
        .iter()
        .find_map(|name| std::env::var(name).ok()?.parse().ok())
        .unwrap_or(default)
}

fn env_u32(name: &str, default: u32) -> u32 {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}

fn env_usize_list(name: &str, default: &[usize]) -> Vec<usize> {
    std::env::var(name)
        .ok()
        .map(|value| {
            value
                .split(',')
                .filter_map(|part| part.trim().parse().ok())
                .collect::<Vec<_>>()
        })
        .filter(|values| !values.is_empty())
        .unwrap_or_else(|| default.to_vec())
}

fn env_f64(name: &str, default: f64) -> f64 {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}

fn env_f64_any(names: &[&str], default: f64) -> f64 {
    names
        .iter()
        .find_map(|name| std::env::var(name).ok()?.parse().ok())
        .unwrap_or(default)
}

fn env_present_any(names: &[&str]) -> bool {
    names.iter().any(|name| std::env::var(name).is_ok())
}

fn env_f64_list(name: &str, default: &[f64]) -> Vec<f64> {
    std::env::var(name)
        .ok()
        .map(|value| {
            value
                .split(',')
                .filter_map(|part| part.trim().parse().ok())
                .collect::<Vec<_>>()
        })
        .filter(|values| !values.is_empty())
        .unwrap_or_else(|| default.to_vec())
}

fn codec_enabled(env_name: &str, codec: &str) -> bool {
    std::env::var(env_name).map_or(true, |value| {
        value
            .split(',')
            .map(|part| part.trim())
            .any(|part| part.eq_ignore_ascii_case("all") || part.eq_ignore_ascii_case(codec))
    })
}

fn env_loss_decode_budget() -> LossDecodeBudget {
    let fixed_overhead_envs = ["CODEC_LOSS_TARGET_RX_OVERHEAD", "CODEC_LOSS_RX_OVERHEAD"];
    let default_mode = if env_present_any(&fixed_overhead_envs) {
        "fixed_target_rx_overhead"
    } else {
        "paper_bec"
    };
    let mode = std::env::var("CODEC_LOSS_BUDGET_MODE").unwrap_or_else(|_| default_mode.to_string());
    match mode.trim().to_ascii_lowercase().as_str() {
        "paper" | "paper_bec" | "paper-ceiling" | "paper_ceiling" => LossDecodeBudget::PaperBec,
        "fixed" | "fixed_target_rx_overhead" | "target" | "target_rx_overhead" => {
            LossDecodeBudget::FixedTargetRxOverhead(env_f64_any(&fixed_overhead_envs, 0.05))
        }
        other => panic!(
            "CODEC_LOSS_BUDGET_MODE must be paper_bec or fixed_target_rx_overhead, got {other}"
        ),
    }
}

fn target_rx_overhead(
    budget: LossDecodeBudget,
    codec: Codec,
    loss_rate: f64,
    margin: f64,
) -> Option<f64> {
    budget
        .base_target_rx_overhead(codec, loss_rate)
        .map(|overhead| (overhead + margin).max(0.0))
}

fn source_symbol(source_id: usize, symbol_size: usize) -> Vec<u8> {
    (0..symbol_size)
        .map(|byte_index| {
            source_id
                .wrapping_mul(31)
                .wrapping_add(byte_index.wrapping_mul(17)) as u8
        })
        .collect()
}

fn source_symbols(k: usize, symbol_size: usize) -> Vec<Vec<u8>> {
    (0..k)
        .map(|source_id| source_symbol(source_id, symbol_size))
        .collect()
}

fn flat_source_data(k: usize, symbol_size: usize) -> Vec<u8> {
    source_symbols(k, symbol_size)
        .into_iter()
        .flatten()
        .collect()
}

fn raptorq_oti(k: usize, symbol_size: usize) -> ObjectTransmissionInformation {
    ObjectTransmissionInformation::new((k * symbol_size) as u64, symbol_size as u16, 1, 1, 1)
}

fn raptorq_single_block_supported(k: usize) -> bool {
    // RFC 6330/RaptorQ limits a single source block to 56,403 source symbols.
    k <= 56_403
}

fn tx_packets_for_overhead(k: usize, overhead: f64) -> usize {
    ((k as f64) * (1.0 + overhead)).ceil().max(k as f64) as usize
}

fn tx_packets_for_target_rx_overhead(k: usize, loss_rate: f64, target_rx_overhead: f64) -> usize {
    let keep_rate = (1.0 - loss_rate).max(0.000_001);
    ((k as f64) * (1.0 + target_rx_overhead) / keep_rate)
        .ceil()
        .max(k as f64) as usize
}

fn paper_ceiling_bec_overheads(loss_rate: f64) -> Option<PaperBecOverheads> {
    let loss_bps_f64 = loss_rate * 10_000.0;
    let loss_bps = loss_bps_f64.round() as u32;
    if (f64::from(loss_bps) - loss_bps_f64).abs() > 1e-6 {
        return None;
    }

    let (mettle_bps, raptorq_bps) = if loss_bps <= 100 {
        (550, 614)
    } else if loss_bps <= 200 {
        (800, 714)
    } else if loss_bps <= 300 {
        (900, 763)
    } else if loss_bps <= 800 {
        (2_000, 1_560)
    } else if loss_bps <= 1_000 {
        (2_500, 1_500)
    } else {
        return None;
    };

    Some(PaperBecOverheads {
        mettle: f64::from(mettle_bps) / 10_000.0,
        raptorq: f64::from(raptorq_bps) / 10_000.0,
    })
}

fn overhead_ratio_for_tx_packets(k: usize, tx_packets: usize) -> OverheadRatio {
    let repair_packets = tx_packets.saturating_sub(k).max(1);
    OverheadRatio::new(repair_packets as u32, k as u32).expect("valid overhead ratio")
}

fn raptorq_tx_packets(k: usize, symbol_size: usize, tx_packets: usize) -> Vec<EncodingPacket> {
    let oti = raptorq_oti(k, symbol_size);
    let flat_data = flat_source_data(k, symbol_size);
    let encoder = SourceBlockEncoder::new(0, &oti, &flat_data);
    let repair_packets = tx_packets.saturating_sub(k);
    let mut packets = encoder.source_packets();
    packets.extend(encoder.repair_packets(0, repair_packets as u32));
    packets
}

fn mettle_tx_packets(
    k: usize,
    symbol_size: usize,
    tx_packets: usize,
    seed: u64,
) -> (MettleParams, NonZeroUsize, Vec<(u128, Vec<u8>)>) {
    // `tx_packets` is the nominal budget used to configure METTLE's density.
    // A terminated stream may emit more bins because the tail is finite and
    // must be flushed; report both values in the CSV instead of conflating them.
    let params = MettleParams::new(overhead_ratio_for_tx_packets(k, tx_packets));
    let source_symbol_bytes = NonZeroUsize::new(symbol_size).expect("non-zero symbol size");
    let mut encoder = TestEncoder::new_terminated(params, source_symbol_bytes, seed, k as u64);
    let mut packets = Vec::new();
    for source in source_symbols(k, symbol_size) {
        packets.extend(encoder.push_source(&source));
    }
    packets.extend(encoder.finish());
    (params, source_symbol_bytes, packets)
}

fn assert_rolling_mettle_decoder(decoder: &TestDecoder) {
    assert_eq!(
        decoder.stats().graph_bins,
        None,
        "codec throughput harness must use the paper-native rolling METTLE decoder"
    );
}

struct MettlePaperGraph {
    source_edges: Vec<Vec<u128>>,
    bin_touchers: HashMap<u128, Vec<usize>>,
}

impl MettlePaperGraph {
    fn new(params: MettleParams, seed: u64, source_count: usize) -> Self {
        let terminal_source_count = source_count as u64;
        let mut source_edges = Vec::with_capacity(source_count);
        let mut bin_touchers = HashMap::<u128, Vec<usize>>::new();

        for source_id in 0..terminal_source_count {
            let mut edges = Vec::<u128>::with_capacity(MettleParams::EDGE_COUNT);
            for bin_id in edge_bin_ids_with_terminal_source_count(
                params,
                source_id,
                seed,
                Some(terminal_source_count),
            ) {
                if edges.contains(&bin_id) {
                    continue;
                }
                edges.push(bin_id);
                bin_touchers
                    .entry(bin_id)
                    .or_default()
                    .push(source_id as usize);
            }
            source_edges.push(edges);
        }

        Self {
            source_edges,
            bin_touchers,
        }
    }

    fn classify(&self, delivered_bin_ids: &HashSet<u128>) -> MettlePaperOutcome {
        let mut remaining_touchers = delivered_bin_ids
            .iter()
            .filter_map(|bin_id| {
                self.bin_touchers
                    .get(bin_id)
                    .map(|touchers| (*bin_id, touchers.len()))
            })
            .collect::<HashMap<_, _>>();
        let mut queue = remaining_touchers
            .iter()
            .filter_map(|(&bin_id, &count)| (count == 1).then_some(bin_id))
            .collect::<VecDeque<_>>();
        let mut decoded = vec![false; self.source_edges.len()];

        while let Some(bin_id) = queue.pop_front() {
            if remaining_touchers.get(&bin_id).copied() != Some(1) {
                continue;
            }
            let Some(source_id) = self.bin_touchers.get(&bin_id).and_then(|touchers| {
                touchers
                    .iter()
                    .copied()
                    .find(|&source_id| !decoded[source_id])
            }) else {
                continue;
            };
            decoded[source_id] = true;
            for &edge_bin_id in &self.source_edges[source_id] {
                let Some(count) = remaining_touchers.get_mut(&edge_bin_id) else {
                    continue;
                };
                if *count == 0 {
                    continue;
                }
                *count -= 1;
                if *count == 1 {
                    queue.push_back(edge_bin_id);
                }
            }
        }

        let mut non_isolated_residual_sources = 0usize;
        let mut isolated_sources = 0usize;
        for (source_id, edges) in self.source_edges.iter().enumerate() {
            if decoded[source_id] {
                continue;
            }
            if edges
                .iter()
                .all(|bin_id| !delivered_bin_ids.contains(bin_id))
            {
                isolated_sources += 1;
            } else {
                non_isolated_residual_sources += 1;
            }
        }

        MettlePaperOutcome {
            non_isolated_residual_sources,
            isolated_sources,
        }
    }
}

fn select_raptorq_survivors(
    packets: &[EncodingPacket],
    loss_rate: f64,
    trial: usize,
) -> Vec<EncodingPacket> {
    let trial_seed = (trial as u64 + 1).wrapping_mul(0x9e37_79b9_7f4a_7c15);
    let mut prng = Prng::new(DEFAULT_SEED ^ trial_seed);
    packets
        .iter()
        .filter(|_| prng.keep_packet(loss_rate))
        .cloned()
        .collect()
}

fn select_mettle_survivors(
    packets: &[(u128, Vec<u8>)],
    loss_rate: f64,
    trial: usize,
) -> Vec<(u128, Vec<u8>)> {
    let trial_seed = (trial as u64 + 1).wrapping_mul(0xd1b5_4a32_d192_ed03);
    let mut prng = Prng::new(DEFAULT_SEED ^ trial_seed);
    packets
        .iter()
        .filter(|_| prng.keep_packet(loss_rate))
        .cloned()
        .collect()
}

fn select_mettle_survivor_id_set(
    packet_ids: &[u128],
    loss_rate: f64,
    trial: usize,
) -> HashSet<u128> {
    let trial_seed = (trial as u64 + 1).wrapping_mul(0xd1b5_4a32_d192_ed03);
    let mut prng = Prng::new(DEFAULT_SEED ^ trial_seed);
    packet_ids
        .iter()
        .copied()
        .filter(|_| prng.keep_packet(loss_rate))
        .collect()
}

fn select_mettle_lost_id_set(
    packet_ids: &[u128],
    loss_rate: f64,
    trial: usize,
) -> (HashSet<u128>, usize) {
    let trial_seed = (trial as u64 + 1).wrapping_mul(0xd1b5_4a32_d192_ed03);
    let mut prng = Prng::new(DEFAULT_SEED ^ trial_seed);
    let mut lost_bin_ids = HashSet::new();
    let mut received_packets = 0usize;

    for &packet_id in packet_ids {
        if prng.keep_packet(loss_rate) {
            received_packets += 1;
        } else {
            lost_bin_ids.insert(packet_id);
        }
    }

    (lost_bin_ids, received_packets)
}

fn raptorq_repair_only_decode_fixture(
    k: usize,
    symbol_size: usize,
) -> (ObjectTransmissionInformation, u64, Vec<EncodingPacket>) {
    let oti = raptorq_oti(k, symbol_size);
    let flat_data = flat_source_data(k, symbol_size);
    let encoder = SourceBlockEncoder::new(0, &oti, &flat_data);
    let packets = encoder.repair_packets(0, k as u32);
    let decoded = SourceBlockDecoder::new(0, &oti, flat_data.len() as u64)
        .decode(packets.clone())
        .expect("RaptorQ repair-only fixture should decode");
    assert_eq!(decoded, flat_data);
    (oti, flat_data.len() as u64, packets)
}

fn mettle_decode_fixture(
    k: usize,
    config: DecodeConfig,
) -> (MettleParams, NonZeroUsize, Vec<(u128, Vec<u8>)>) {
    let params = MettleParams::new(config.mettle_overhead);
    let source_symbol_bytes = NonZeroUsize::new(config.symbol_size).expect("non-zero symbol size");
    let mut encoder = TestEncoder::new(params, source_symbol_bytes, DEFAULT_SEED);
    let mut bins = Vec::new();

    for source in source_symbols(k, config.symbol_size) {
        bins.extend(encoder.push_source(&source));
    }

    let mut decoder = TestDecoder::new(params, source_symbol_bytes, DEFAULT_SEED);
    let mut decoded_sources = 0usize;
    let mut decodable_prefix = Vec::new();
    for (bin_id, payload) in bins {
        decoded_sources += decoder.push_bin(bin_id, payload.clone()).len();
        decodable_prefix.push((bin_id, payload));
        if decoded_sources == k {
            return (params, source_symbol_bytes, decodable_prefix);
        }
    }

    panic!("METTLE streaming fixture did not decode the requested prefix");
}

fn measure_raptorq_decode(k: usize, config: DecodeConfig) -> DecodeMetrics {
    let (oti, block_length, packets) = raptorq_repair_only_decode_fixture(k, config.symbol_size);
    let received_packets = packets.len();
    let runs = (0..config.iterations)
        .map(|_| packets.clone())
        .collect::<Vec<_>>();

    let mut elapsed = Duration::ZERO;
    for packets in runs {
        let mut decoder = SourceBlockDecoder::new(0, &oti, block_length);
        let start = Instant::now();
        let decoded = decoder
            .decode(packets)
            .expect("RaptorQ decode fixture should decode");
        elapsed += start.elapsed();
        assert_eq!(decoded.len(), k * config.symbol_size);
        std::hint::black_box(decoded);
    }

    DecodeMetrics {
        received_packets,
        elapsed,
    }
}

fn measure_mettle_decode(k: usize, config: DecodeConfig) -> DecodeMetrics {
    let (params, source_symbol_bytes, bins) = mettle_decode_fixture(k, config);
    let received_packets = bins.len();
    let runs = (0..config.iterations)
        .map(|_| bins.clone())
        .collect::<Vec<_>>();

    let mut elapsed = Duration::ZERO;
    for bins in runs {
        let mut decoder = TestDecoder::new(params, source_symbol_bytes, DEFAULT_SEED);
        let mut decoded_sources = 0usize;
        let start = Instant::now();
        for (bin_id, payload) in bins {
            decoded_sources += decoder.push_bin(bin_id, payload).len();
        }
        elapsed += start.elapsed();
        assert_eq!(decoded_sources, k);
        std::hint::black_box(decoded_sources);
    }

    DecodeMetrics {
        received_packets,
        elapsed,
    }
}

fn measure_raptorq_encode(
    k: usize,
    symbol_size: usize,
    overhead: f64,
    iterations: usize,
) -> Option<EncodeMetrics> {
    if !raptorq_single_block_supported(k) {
        return None;
    }
    let tx_packets = tx_packets_for_overhead(k, overhead);
    let oti = raptorq_oti(k, symbol_size);
    let flat_data = flat_source_data(k, symbol_size);
    let repair_packets = tx_packets.saturating_sub(k);
    let start = Instant::now();
    for _ in 0..iterations {
        let encoder = SourceBlockEncoder::new(0, &oti, &flat_data);
        let source_packets = encoder.source_packets();
        let repair_packets = encoder.repair_packets(0, repair_packets as u32);
        std::hint::black_box(source_packets);
        std::hint::black_box(repair_packets);
    }
    Some(EncodeMetrics {
        configured_tx_packets: tx_packets,
        tx_packets,
        elapsed: start.elapsed(),
    })
}

fn measure_mettle_encode(
    k: usize,
    symbol_size: usize,
    overhead: f64,
    iterations: usize,
) -> EncodeMetrics {
    let tx_packets = tx_packets_for_overhead(k, overhead);
    let params = MettleParams::new(overhead_ratio_for_tx_packets(k, tx_packets));
    let source_symbol_bytes = NonZeroUsize::new(symbol_size).expect("non-zero symbol size");
    let sources = source_symbols(k, symbol_size);
    let start = Instant::now();
    let mut emitted_packets = 0usize;
    for _ in 0..iterations {
        let mut encoder =
            TestEncoder::new_terminated(params, source_symbol_bytes, DEFAULT_SEED, k as u64);
        let mut emitted = 0usize;
        for source in &sources {
            emitted += encoder.push_source(source).len();
        }
        emitted += encoder.finish().len();
        emitted_packets = emitted;
        std::hint::black_box(emitted);
    }
    EncodeMetrics {
        configured_tx_packets: tx_packets,
        tx_packets: emitted_packets,
        elapsed: start.elapsed(),
    }
}

fn measure_raptorq_decode_with_loss(
    k: usize,
    loss_rate: f64,
    target_rx_overhead: f64,
    config: LossDecodeConfig,
) -> Option<LossDecodeMetrics> {
    if !raptorq_single_block_supported(k) {
        return None;
    }
    let tx_packets = tx_packets_for_target_rx_overhead(k, loss_rate, target_rx_overhead);
    let packets = raptorq_tx_packets(k, config.symbol_size, tx_packets);
    let oti = raptorq_oti(k, config.symbol_size);
    let block_length = (k * config.symbol_size) as u64;
    let mut metrics = LossDecodeMetrics::new(config.trials, tx_packets, packets.len());

    for trial in 0..config.trials {
        let survivors = select_raptorq_survivors(&packets, loss_rate, trial);
        metrics.total_received_packets += survivors.len();
        let received_packets = survivors.len();
        let mut decoder = SourceBlockDecoder::new(0, &oti, block_length);
        let start = Instant::now();
        let decoded = decoder.decode(survivors);
        let elapsed = start.elapsed();
        if let Some(decoded) = decoded {
            if decoded.len() == k * config.symbol_size {
                metrics.record_strict_success();
                metrics.record_paper_success(received_packets, elapsed);
                std::hint::black_box(decoded);
            }
        }
    }

    Some(metrics)
}

fn measure_mettle_decode_with_loss(
    k: usize,
    loss_rate: f64,
    target_rx_overhead: f64,
    config: LossDecodeConfig,
) -> LossDecodeMetrics {
    let tx_packets = tx_packets_for_target_rx_overhead(k, loss_rate, target_rx_overhead);
    let (params, source_symbol_bytes, packets) =
        mettle_tx_packets(k, config.symbol_size, tx_packets, DEFAULT_SEED);
    let paper_graph = MettlePaperGraph::new(params, DEFAULT_SEED, k);
    let mut metrics = LossDecodeMetrics::new(config.trials, tx_packets, packets.len());

    for trial in 0..config.trials {
        let survivors = select_mettle_survivors(&packets, loss_rate, trial);
        metrics.total_received_packets += survivors.len();
        let received_packets = survivors.len();
        let delivered_bin_ids = survivors
            .iter()
            .map(|(bin_id, _)| *bin_id)
            .collect::<HashSet<_>>();
        let mut decoder =
            TestDecoder::new_terminated(params, source_symbol_bytes, DEFAULT_SEED, k as u64);
        assert_rolling_mettle_decoder(&decoder);
        let start = Instant::now();
        for (bin_id, payload) in survivors {
            let _ = decoder.push_bin(bin_id, payload);
        }
        let elapsed = start.elapsed();
        let strict_success = decoder.next_source_id() == k as u64;
        let paper_outcome = paper_graph.classify(&delivered_bin_ids);
        debug_assert!(
            !strict_success
                || matches!(
                    paper_outcome.classify(config.mettle_local_residual_source_limit),
                    MettlePaperOutcomeClass::FullDecode
                ),
            "strict METTLE decode succeeded but offline graph classifier reported residuals"
        );
        metrics.record_mettle_outcome(
            paper_outcome,
            config.mettle_local_residual_source_limit,
            received_packets,
            elapsed,
            strict_success,
        );
    }

    metrics
}

fn measure_raptorq_efficiency(
    k: usize,
    loss_rate: f64,
    overhead: f64,
    config: EfficiencyConfig,
) -> Option<EfficiencyMetrics> {
    if !raptorq_single_block_supported(k) {
        return None;
    }
    let tx_packets = tx_packets_for_overhead(k, overhead);
    let packets = raptorq_tx_packets(k, config.symbol_size, tx_packets);
    let oti = raptorq_oti(k, config.symbol_size);
    let block_length = (k * config.symbol_size) as u64;
    let mut successes = 0usize;
    let mut rx_packets = 0usize;
    for trial in 0..config.trials {
        let survivors = select_raptorq_survivors(&packets, loss_rate, trial);
        rx_packets += survivors.len();
        let decoded = SourceBlockDecoder::new(0, &oti, block_length).decode(survivors);
        if decoded.is_some_and(|decoded| decoded.len() == k * config.symbol_size) {
            successes += 1;
        }
    }
    Some(EfficiencyMetrics {
        trials: config.trials,
        successes,
        avg_tx_packets: tx_packets as f64,
        avg_rx_packets: rx_packets as f64 / config.trials as f64,
    })
}

fn measure_mettle_efficiency(
    k: usize,
    loss_rate: f64,
    overhead: f64,
    config: EfficiencyConfig,
) -> EfficiencyMetrics {
    let tx_packets = tx_packets_for_overhead(k, overhead);
    let (params, source_symbol_bytes, packets) =
        mettle_tx_packets(k, config.symbol_size, tx_packets, DEFAULT_SEED);
    let mut successes = 0usize;
    let mut rx_packets = 0usize;
    for trial in 0..config.trials {
        let survivors = select_mettle_survivors(&packets, loss_rate, trial);
        rx_packets += survivors.len();
        let mut decoder =
            TestDecoder::new_terminated(params, source_symbol_bytes, DEFAULT_SEED, k as u64);
        for (bin_id, payload) in survivors {
            let _ = decoder.push_bin(bin_id, payload);
        }
        if decoder.next_source_id() == k as u64 {
            successes += 1;
        }
    }
    EfficiencyMetrics {
        trials: config.trials,
        successes,
        avg_tx_packets: packets.len() as f64,
        avg_rx_packets: rx_packets as f64 / config.trials as f64,
    }
}

fn gbps(bits: u128, elapsed: Duration) -> f64 {
    if elapsed.is_zero() {
        return 0.0;
    }
    bits as f64 / elapsed.as_secs_f64() / 1_000_000_000.0
}

fn report_decode_row(codec: &str, k: usize, metrics: DecodeMetrics, config: DecodeConfig) {
    let total_received_packets = config.iterations * metrics.received_packets;
    let total_source_bits = (config.iterations * k * config.symbol_size * 8) as u128;
    let total_rx_bits = (total_received_packets * config.symbol_size * 8) as u128;
    let us_per_received_packet =
        metrics.elapsed.as_secs_f64() * 1_000_000.0 / total_received_packets as f64;
    let rx_overhead_pct = (metrics.received_packets as f64 / k as f64 - 1.0) * 100.0;

    eprintln!(
        "{codec},{k},{},{},{rx_overhead_pct:.3},{us_per_received_packet:.3},{:.3},{:.3}",
        config.symbol_size,
        metrics.received_packets,
        gbps(total_rx_bits, metrics.elapsed),
        gbps(total_source_bits, metrics.elapsed),
    );
}

fn run_decode_case(k: usize, config: DecodeConfig) {
    if raptorq_single_block_supported(k) {
        report_decode_row("raptorq", k, measure_raptorq_decode(k, config), config);
    } else {
        eprintln!(
            "raptorq,{k},{},unsupported_single_block,unsupported_single_block,unsupported_single_block,unsupported_single_block,unsupported_single_block",
            config.symbol_size,
        );
    }
    report_decode_row("mettle", k, measure_mettle_decode(k, config), config);
}

fn report_encode_row(
    codec: &str,
    k: usize,
    symbol_size: usize,
    overhead: f64,
    iterations: usize,
    metrics: EncodeMetrics,
) {
    let total_source_bits = (iterations * k * symbol_size * 8) as u128;
    let total_tx_bits = (iterations * metrics.tx_packets * symbol_size * 8) as u128;
    let actual_tx_overhead = metrics.tx_packets as f64 / k as f64 - 1.0;
    eprintln!(
        "{codec},{k},{symbol_size},{overhead:.4},{},{:.3},{:.3},{},{actual_tx_overhead:.6}",
        metrics.tx_packets,
        gbps(total_source_bits, metrics.elapsed),
        gbps(total_tx_bits, metrics.elapsed),
        metrics.configured_tx_packets,
    );
}

fn report_loss_decode_row(
    codec: &str,
    k: usize,
    loss_rate: f64,
    target_rx_overhead: f64,
    config: LossDecodeConfig,
    metrics: LossDecodeMetrics,
) {
    let avg_rx_packets = metrics.total_received_packets as f64 / metrics.trials as f64;
    let avg_success_rx_packets = if metrics.paper_successes == 0 {
        f64::NAN
    } else {
        metrics.successful_received_packets as f64 / metrics.paper_successes as f64
    };
    let avg_rx_overhead = avg_rx_packets / k as f64 - 1.0;
    let avg_success_rx_overhead = avg_success_rx_packets / k as f64 - 1.0;
    let successful_rx_bits = (metrics.successful_received_packets * config.symbol_size * 8) as u128;
    let successful_source_bits = (metrics.paper_successes * k * config.symbol_size * 8) as u128;
    let success_rate = metrics.paper_successes as f64 / metrics.trials as f64;
    let strict_success_rate = metrics.strict_successes as f64 / metrics.trials as f64;
    let avg_non_isolated_residual_sources =
        metrics.non_isolated_residual_sources_total as f64 / metrics.trials as f64;
    let avg_isolated_sources = metrics.isolated_sources_total as f64 / metrics.trials as f64;
    let us_per_success_rx_packet = metrics.successful_elapsed.as_secs_f64() * 1_000_000.0
        / metrics.successful_received_packets.max(1) as f64;
    let rx_processing_throughput_gbps = gbps(successful_rx_bits, metrics.successful_elapsed);
    let decode_throughput_gbps = gbps(successful_source_bits, metrics.successful_elapsed);
    let actual_tx_overhead = metrics.tx_packets as f64 / k as f64 - 1.0;
    eprintln!(
        "{codec},{k},{},{loss_rate:.4},{target_rx_overhead:.4},{},{},ok,{},{success_rate:.6},{},{strict_success_rate:.6},{},{},{},{},{avg_non_isolated_residual_sources:.3},{},{avg_isolated_sources:.3},{},{},{avg_rx_packets:.1},{avg_success_rx_packets:.1},{avg_rx_overhead:.6},{avg_success_rx_overhead:.6},{us_per_success_rx_packet:.3},{rx_processing_throughput_gbps:.3},{decode_throughput_gbps:.3},{},{},{actual_tx_overhead:.6}",
        config.symbol_size,
        config.budget.label(),
        metrics.trials,
        metrics.paper_successes,
        metrics.strict_successes,
        metrics.isolated_error_floor_events,
        metrics.local_residual_events,
        metrics.stall_failures,
        metrics.mixed_failures,
        metrics.non_isolated_residual_sources_max,
        metrics.isolated_sources_max,
        config.mettle_local_residual_source_limit,
        metrics.configured_tx_packets,
        metrics.tx_packets,
    );
}

fn report_loss_decode_unsupported(
    codec: &str,
    k: usize,
    loss_rate: f64,
    config: LossDecodeConfig,
    reason: &str,
) {
    let unsupported_tail = std::iter::repeat("unsupported")
        .take(23)
        .collect::<Vec<_>>()
        .join(",");
    eprintln!(
        "{codec},{k},{},{loss_rate:.4},unsupported,{},{},{reason},{unsupported_tail}",
        config.symbol_size,
        config.budget.label(),
        config.trials,
    );
}

fn report_efficiency_row(
    codec: &str,
    k: usize,
    loss_rate: f64,
    overhead: f64,
    config: EfficiencyConfig,
    metrics: EfficiencyMetrics,
) {
    eprintln!(
        "{codec},{k},{},{loss_rate:.4},{overhead:.4},{},{},{:.1},{:.1},{:.6}",
        config.symbol_size,
        metrics.trials,
        metrics.successes,
        metrics.avg_tx_packets,
        metrics.avg_rx_packets,
        1.0 - metrics.successes as f64 / metrics.trials as f64,
    );
}

#[test]
fn codec_decode_speed_harness_builds() {
    let config = DecodeConfig {
        symbol_size: 256,
        iterations: 1,
        mettle_overhead: OverheadRatio::new(1, 20).expect("valid overhead"),
    };
    run_decode_case(128, config);
}

#[test]
#[ignore = "manual pure-codec decode-speed benchmark"]
fn report_codec_decode_speed() {
    let ks = env_usize_list("CODEC_DECODE_KS", &[127, 257, 511, 1002, 2040, 4069, 8194]);
    let config = DecodeConfig {
        symbol_size: env_usize("CODEC_DECODE_SYMBOL_SIZE", DEFAULT_SYMBOL_SIZE),
        iterations: env_usize("CODEC_DECODE_ITERATIONS", 5),
        mettle_overhead: OverheadRatio::new(
            env_u32(
                "CODEC_DECODE_METTLE_OVERHEAD_NUMERATOR",
                DEFAULT_METTLE_OVERHEAD_NUMERATOR,
            ),
            env_u32(
                "CODEC_DECODE_METTLE_OVERHEAD_DENOMINATOR",
                DEFAULT_METTLE_OVERHEAD_DENOMINATOR,
            ),
        )
        .expect("valid METTLE overhead"),
    };

    eprintln!(
        "codec,k,symbol_size,received_packets,rx_overhead_pct,decode_us_per_received_packet,decode_rx_gbps,decode_source_gbps"
    );
    for k in ks {
        run_decode_case(k, config);
    }
}

#[test]
#[ignore = "manual pure-codec encode-speed surface"]
fn report_codec_encode_surface() {
    let ks = env_usize_list(
        "CODEC_ENCODE_KS",
        &[1024, 2048, 4096, 8192, 16384, 32768, 56300, 100000],
    );
    let symbol_sizes = env_usize_list("CODEC_ENCODE_SYMBOL_SIZES", &[1500, 8192]);
    let iterations = env_usize("CODEC_ENCODE_ITERATIONS", 3);
    let overhead = env_f64("CODEC_ENCODE_OVERHEAD", 0.05);

    eprintln!(
        "codec,k,symbol_size,overhead,tx_packets,encode_source_gbps,encode_tx_gbps,configured_tx_packets,actual_tx_overhead"
    );
    for symbol_size in symbol_sizes {
        for &k in &ks {
            if let Some(metrics) = measure_raptorq_encode(k, symbol_size, overhead, iterations) {
                report_encode_row("raptorq", k, symbol_size, overhead, iterations, metrics);
            } else {
                eprintln!(
                    "raptorq,{k},{symbol_size},{overhead:.4},unsupported_single_block,unsupported_single_block,unsupported_single_block,unsupported_single_block,unsupported_single_block"
                );
            }
            let metrics = measure_mettle_encode(k, symbol_size, overhead, iterations);
            report_encode_row("mettle", k, symbol_size, overhead, iterations, metrics);
        }
    }
}

#[test]
#[ignore = "manual pure-codec decode surface under BEC loss"]
fn report_codec_decode_loss_surface() {
    let ks = env_usize_list("CODEC_LOSS_KS", &[1024, 2048, 4096]);
    let loss_rates = env_f64_list("CODEC_LOSS_RATES", DEFAULT_DECODE_LOSS_RATES);
    let config = LossDecodeConfig {
        symbol_size: env_usize("CODEC_LOSS_SYMBOL_SIZE", DEFAULT_SYMBOL_SIZE),
        trials: env_usize("CODEC_LOSS_TRIALS", 2),
        budget: env_loss_decode_budget(),
        overhead_margin: env_f64("CODEC_LOSS_OVERHEAD_MARGIN", 0.0),
        mettle_local_residual_source_limit: env_usize_any(
            &[
                "CODEC_LOSS_METTLE_LOCAL_RESIDUAL_SOURCE_LIMIT",
                "METTLE_TABLE_IV_LOCAL_RESIDUAL_SOURCE_LIMIT",
                "METTLE_TABLE_IV_LOCAL_ERROR_FLOOR_SOURCE_LIMIT",
            ],
            100,
        ),
    };

    eprintln!(
        "codec,k,symbol_size,loss,target_rx_overhead,budget_mode,trials,status,successes,success_rate,strict_successes,strict_success_rate,isolated_error_floor_events,local_residual_events,stall_failures,mixed_failures,avg_non_isolated_residual_sources,max_non_isolated_residual_sources,avg_isolated_sources,max_isolated_sources,local_residual_source_limit,avg_rx_packets,avg_success_rx_packets,avg_rx_overhead,avg_success_rx_overhead,decode_us_per_success_rx_packet,rx_processing_throughput_gbps,decode_throughput_gbps,configured_tx_packets,tx_packets,actual_tx_overhead"
    );
    for k in ks {
        for &loss_rate in &loss_rates {
            if let Some(target_rx_overhead) = target_rx_overhead(
                config.budget,
                Codec::Raptorq,
                loss_rate,
                config.overhead_margin,
            ) {
                if let Some(metrics) =
                    measure_raptorq_decode_with_loss(k, loss_rate, target_rx_overhead, config)
                {
                    report_loss_decode_row(
                        "raptorq",
                        k,
                        loss_rate,
                        target_rx_overhead,
                        config,
                        metrics,
                    );
                } else {
                    report_loss_decode_unsupported(
                        "raptorq",
                        k,
                        loss_rate,
                        config,
                        "unsupported_single_block",
                    );
                }
            } else {
                report_loss_decode_unsupported(
                    "raptorq",
                    k,
                    loss_rate,
                    config,
                    "unsupported_paper_bec_loss",
                );
            }

            if let Some(target_rx_overhead) = target_rx_overhead(
                config.budget,
                Codec::Mettle,
                loss_rate,
                config.overhead_margin,
            ) {
                report_loss_decode_row(
                    "mettle",
                    k,
                    loss_rate,
                    target_rx_overhead,
                    config,
                    measure_mettle_decode_with_loss(k, loss_rate, target_rx_overhead, config),
                );
            } else {
                report_loss_decode_unsupported(
                    "mettle",
                    k,
                    loss_rate,
                    config,
                    "unsupported_paper_bec_loss",
                );
            }
        }
    }
}

#[test]
#[ignore = "manual METTLE low-loss isolated-error-floor smoke surface"]
fn report_mettle_low_loss_isolation_surface() {
    let ks = env_usize_list(
        "METTLE_ISOLATION_KS",
        &[128, 256, 512, 1024, 2048, 4096, 8192, 16384, 32768, 65536],
    );
    let loss_rates = env_f64_list(
        "METTLE_ISOLATION_LOSS_RATES",
        &[
            0.0010, 0.0020, 0.0030, 0.0050, 0.0075, 0.0100, 0.0125, 0.0150, 0.0175, 0.0200,
        ],
    );
    let trials = env_usize("METTLE_ISOLATION_TRIALS", 1000);
    let budget = env_loss_decode_budget();
    let overhead_margin = env_f64("METTLE_ISOLATION_OVERHEAD_MARGIN", 0.0);
    let local_residual_source_limit = env_usize_any(
        &[
            "METTLE_ISOLATION_LOCAL_RESIDUAL_SOURCE_LIMIT",
            "CODEC_LOSS_METTLE_LOCAL_RESIDUAL_SOURCE_LIMIT",
            "METTLE_TABLE_IV_LOCAL_RESIDUAL_SOURCE_LIMIT",
            "METTLE_TABLE_IV_LOCAL_ERROR_FLOOR_SOURCE_LIMIT",
        ],
        100,
    );

    eprintln!(
        "k,loss,target_rx_overhead,budget_mode,trials,status,full_decodes,isolated_error_floor_events,local_residual_events,stall_failures,mixed_failures,avg_isolated_sources,max_isolated_sources,avg_non_isolated_residual_sources,max_non_isolated_residual_sources,avg_rx_packets,avg_rx_overhead,local_residual_source_limit,configured_tx_packets,tx_packets,actual_tx_overhead,elapsed_ms"
    );

    for k in ks {
        for &loss_rate in &loss_rates {
            let Some(target_rx_overhead) =
                target_rx_overhead(budget, Codec::Mettle, loss_rate, overhead_margin)
            else {
                eprintln!(
                    "{k},{loss_rate:.4},unsupported,{},{trials},unsupported_paper_bec_loss,unsupported,unsupported,unsupported,unsupported,unsupported,unsupported,unsupported,unsupported,unsupported,unsupported,unsupported,{local_residual_source_limit},unsupported,unsupported,unsupported,unsupported",
                    budget.label(),
                );
                continue;
            };

            let tx_packets = tx_packets_for_target_rx_overhead(k, loss_rate, target_rx_overhead);
            let (params, _, packets) = mettle_tx_packets(k, 1, tx_packets, DEFAULT_SEED);
            let packet_ids = packets
                .iter()
                .map(|(bin_id, _)| *bin_id)
                .collect::<Vec<_>>();
            let graph = MettlePaperGraph::new(params, DEFAULT_SEED, k);

            let mut full_decodes = 0usize;
            let mut isolated_error_floor_events = 0usize;
            let mut local_residual_events = 0usize;
            let mut stall_failures = 0usize;
            let mut mixed_failures = 0usize;
            let mut isolated_sources_total = 0usize;
            let mut non_isolated_residual_sources_total = 0usize;
            let mut max_isolated_sources = 0usize;
            let mut max_non_isolated_residual_sources = 0usize;
            let mut total_received_packets = 0usize;

            let start = Instant::now();
            for trial in 0..trials {
                let delivered_bin_ids =
                    select_mettle_survivor_id_set(&packet_ids, loss_rate, trial);
                total_received_packets += delivered_bin_ids.len();
                let outcome = graph.classify(&delivered_bin_ids);

                isolated_sources_total += outcome.isolated_sources;
                non_isolated_residual_sources_total += outcome.non_isolated_residual_sources;
                max_isolated_sources = max_isolated_sources.max(outcome.isolated_sources);
                max_non_isolated_residual_sources =
                    max_non_isolated_residual_sources.max(outcome.non_isolated_residual_sources);

                match outcome.classify(local_residual_source_limit) {
                    MettlePaperOutcomeClass::FullDecode => full_decodes += 1,
                    MettlePaperOutcomeClass::IsolatedErrorFloor => {
                        isolated_error_floor_events += 1;
                    }
                    MettlePaperOutcomeClass::LocalResidual => local_residual_events += 1,
                    MettlePaperOutcomeClass::StallFailure => stall_failures += 1,
                    MettlePaperOutcomeClass::MixedFailure => mixed_failures += 1,
                }
            }
            let elapsed = start.elapsed();

            let avg_rx_packets = total_received_packets as f64 / trials as f64;
            let avg_rx_overhead = avg_rx_packets / k as f64 - 1.0;
            let avg_isolated_sources = isolated_sources_total as f64 / trials as f64;
            let avg_non_isolated_residual_sources =
                non_isolated_residual_sources_total as f64 / trials as f64;
            let actual_tx_overhead = packets.len() as f64 / k as f64 - 1.0;

            eprintln!(
                "{k},{loss_rate:.4},{target_rx_overhead:.4},{},{trials},ok,{full_decodes},{isolated_error_floor_events},{local_residual_events},{stall_failures},{mixed_failures},{avg_isolated_sources:.6},{max_isolated_sources},{avg_non_isolated_residual_sources:.6},{max_non_isolated_residual_sources},{avg_rx_packets:.1},{avg_rx_overhead:.6},{local_residual_source_limit},{tx_packets},{},{actual_tx_overhead:.6},{:.3}",
                budget.label(),
                packets.len(),
                elapsed.as_secs_f64() * 1000.0,
            );
        }
    }
}

#[test]
#[ignore = "manual METTLE low-loss isolated-source smoke surface"]
fn report_mettle_low_loss_isolated_source_surface() {
    let ks = env_usize_list(
        "METTLE_ISOLATED_SOURCE_KS",
        &[128, 256, 512, 1024, 2048, 4096, 8192, 16384, 32768, 65536],
    );
    let loss_rates = env_f64_list(
        "METTLE_ISOLATED_SOURCE_LOSS_RATES",
        &[
            0.0010, 0.0020, 0.0030, 0.0050, 0.0075, 0.0100, 0.0125, 0.0150, 0.0175, 0.0200,
        ],
    );
    let trials = env_usize("METTLE_ISOLATED_SOURCE_TRIALS", 2000);
    let budget = env_loss_decode_budget();
    let overhead_margin = env_f64("METTLE_ISOLATED_SOURCE_OVERHEAD_MARGIN", 0.0);

    eprintln!(
        "k,loss,target_rx_overhead,budget_mode,trials,status,isolated_trials,total_isolated_sources,avg_isolated_sources,max_isolated_sources,first_isolated_trial,expected_isolated_sources_per_trial,expected_isolated_trials,avg_lost_packets,max_lost_packets,avg_rx_packets,avg_rx_overhead,configured_tx_packets,tx_packets,actual_tx_overhead,elapsed_ms"
    );

    for k in ks {
        for &loss_rate in &loss_rates {
            let Some(target_rx_overhead) =
                target_rx_overhead(budget, Codec::Mettle, loss_rate, overhead_margin)
            else {
                eprintln!(
                    "{k},{loss_rate:.4},unsupported,{},{trials},unsupported_paper_bec_loss,unsupported,unsupported,unsupported,unsupported,unsupported,unsupported,unsupported,unsupported,unsupported,unsupported,unsupported,unsupported,unsupported,unsupported,unsupported",
                    budget.label(),
                );
                continue;
            };

            let tx_packets = tx_packets_for_target_rx_overhead(k, loss_rate, target_rx_overhead);
            let (params, _, packets) = mettle_tx_packets(k, 1, tx_packets, DEFAULT_SEED);
            let packet_ids = packets
                .iter()
                .map(|(bin_id, _)| *bin_id)
                .collect::<Vec<_>>();
            let emitted_bin_ids = packet_ids.iter().copied().collect::<HashSet<_>>();
            assert_eq!(
                emitted_bin_ids.len(),
                packet_ids.len(),
                "METTLE isolated-source smoke assumes unique emitted bin IDs"
            );

            let graph = MettlePaperGraph::new(params, DEFAULT_SEED, k);
            let expected_isolated_sources_per_trial = graph
                .source_edges
                .iter()
                .map(|edges| {
                    edges.iter().fold(1.0, |probability, bin_id| {
                        if emitted_bin_ids.contains(bin_id) {
                            probability * loss_rate
                        } else {
                            probability
                        }
                    })
                })
                .sum::<f64>();
            let expected_isolated_trials =
                trials as f64 * (1.0 - (-expected_isolated_sources_per_trial).exp());

            let mut isolated_trials = 0usize;
            let mut total_isolated_sources = 0usize;
            let mut max_isolated_sources = 0usize;
            let mut first_isolated_trial = None::<usize>;
            let mut total_lost_packets = 0usize;
            let mut max_lost_packets = 0usize;
            let mut total_received_packets = 0usize;

            let start = Instant::now();
            for trial in 0..trials {
                let (lost_bin_ids, received_packets) =
                    select_mettle_lost_id_set(&packet_ids, loss_rate, trial);
                total_lost_packets += lost_bin_ids.len();
                max_lost_packets = max_lost_packets.max(lost_bin_ids.len());
                total_received_packets += received_packets;

                let candidate_sources = lost_bin_ids
                    .iter()
                    .filter_map(|bin_id| graph.bin_touchers.get(bin_id))
                    .flat_map(|source_ids| source_ids.iter().copied())
                    .collect::<HashSet<_>>();
                let isolated_sources = candidate_sources
                    .into_iter()
                    .filter(|&source_id| {
                        graph.source_edges[source_id].iter().all(|bin_id| {
                            !emitted_bin_ids.contains(bin_id) || lost_bin_ids.contains(bin_id)
                        })
                    })
                    .count();

                if isolated_sources > 0 {
                    isolated_trials += 1;
                    first_isolated_trial.get_or_insert(trial);
                    total_isolated_sources += isolated_sources;
                    max_isolated_sources = max_isolated_sources.max(isolated_sources);
                }
            }
            let elapsed = start.elapsed();

            let avg_isolated_sources = total_isolated_sources as f64 / trials as f64;
            let avg_lost_packets = total_lost_packets as f64 / trials as f64;
            let avg_rx_packets = total_received_packets as f64 / trials as f64;
            let avg_rx_overhead = avg_rx_packets / k as f64 - 1.0;
            let actual_tx_overhead = packets.len() as f64 / k as f64 - 1.0;

            eprintln!(
                "{k},{loss_rate:.4},{target_rx_overhead:.4},{},{trials},ok,{isolated_trials},{total_isolated_sources},{avg_isolated_sources:.6},{max_isolated_sources},{},{expected_isolated_sources_per_trial:.9},{expected_isolated_trials:.3},{avg_lost_packets:.1},{max_lost_packets},{avg_rx_packets:.1},{avg_rx_overhead:.6},{tx_packets},{},{actual_tx_overhead:.6},{:.3}",
                budget.label(),
                first_isolated_trial
                    .map(|trial| trial.to_string())
                    .unwrap_or_else(|| "none".to_string()),
                packets.len(),
                elapsed.as_secs_f64() * 1000.0,
            );
        }
    }
}

#[test]
#[ignore = "manual METTLE lossy decode instrumentation trace"]
fn report_mettle_decode_loss_trace() {
    let k = env_usize("METTLE_TRACE_K", 4096);
    let symbol_size = env_usize("METTLE_TRACE_SYMBOL_SIZE", DEFAULT_SYMBOL_SIZE);
    let loss_rate = env_f64("METTLE_TRACE_LOSS", 0.05);
    let target_rx_overhead = env_f64("METTLE_TRACE_TARGET_RX_OVERHEAD", 0.05);
    let trial = env_usize("METTLE_TRACE_TRIAL", 0);
    let sample_every = env_usize("METTLE_TRACE_SAMPLE_EVERY", 256).max(1);
    let tx_packets = tx_packets_for_target_rx_overhead(k, loss_rate, target_rx_overhead);
    let (params, source_symbol_bytes, packets) =
        mettle_tx_packets(k, symbol_size, tx_packets, DEFAULT_SEED);
    let survivors = select_mettle_survivors(&packets, loss_rate, trial);
    let mut decoder = TestDecoder::new_terminated(
        params,
        source_symbol_bytes,
        DEFAULT_SEED,
        k.try_into().expect("k fits in u64"),
    );
    assert_rolling_mettle_decoder(&decoder);
    let mut elapsed = Duration::ZERO;
    let mut decoded_sources = 0usize;

    eprintln!(
        "event,k,symbol_size,loss,target_rx_overhead,trial,tx_packets,rx_packets,processed_bins,total_decoded,decoded_now,next_source_id,received_bins,ready_bins,seen_bins,decoded_future_sources,decoded_prefix_sources,decoded_prefix_start_source_id,graph_bins,bin_cleanup_frontier,elapsed_ms,us_per_processed_bin"
    );
    for (index, (bin_id, payload)) in survivors.into_iter().enumerate() {
        let start = Instant::now();
        let decoded_now = decoder.push_bin(bin_id, payload).len();
        elapsed += start.elapsed();
        decoded_sources += decoded_now;
        let processed_bins = index + 1;
        if processed_bins.is_multiple_of(sample_every) || decoded_now != 0 {
            let stats = decoder.stats();
            eprintln!(
                "sample,{k},{symbol_size},{loss_rate:.4},{target_rx_overhead:.4},{trial},{},{},{processed_bins},{decoded_sources},{decoded_now},{},{},{},{},{},{},{},{},{},{:.3},{:.3}",
                tx_packets,
                packets.len(),
                stats.next_source_id,
                stats.received_bins,
                stats.ready_bins,
                stats.seen_bins,
                stats.decoded_future_sources,
                stats.decoded_prefix_sources,
                stats.decoded_prefix_start_source_id,
                stats.graph_bins.unwrap_or(0),
                stats.bin_cleanup_frontier,
                elapsed.as_secs_f64() * 1_000.0,
                elapsed.as_secs_f64() * 1_000_000.0 / processed_bins as f64,
            );
        }
    }

    let stats = decoder.stats();
    let success = usize::from(stats.next_source_id == k as u64);
    eprintln!(
        "final,{k},{symbol_size},{loss_rate:.4},{target_rx_overhead:.4},{trial},{},{},{},{decoded_sources},{success},{},{},{},{},{},{},{},{},{},{:.3},{:.3}",
        tx_packets,
        packets.len(),
        stats.seen_bins,
        stats.next_source_id,
        stats.received_bins,
        stats.ready_bins,
        stats.seen_bins,
        stats.decoded_future_sources,
        stats.decoded_prefix_sources,
        stats.decoded_prefix_start_source_id,
        stats.graph_bins.unwrap_or(0),
        stats.bin_cleanup_frontier,
        elapsed.as_secs_f64() * 1_000.0,
        elapsed.as_secs_f64() * 1_000_000.0 / stats.seen_bins.max(1) as f64,
    );
}

#[test]
#[ignore = "manual strict codec coding-efficiency surface"]
fn report_codec_coding_efficiency_surface() {
    let ks = env_usize_list("CODEC_EFFICIENCY_KS", &[512, 1024, 2048]);
    let loss_rates = env_f64_list("CODEC_EFFICIENCY_LOSS_RATES", DEFAULT_EFFICIENCY_LOSS_RATES);
    let overheads = env_f64_list("CODEC_EFFICIENCY_OVERHEADS", DEFAULT_EFFICIENCY_OVERHEADS);
    let config = EfficiencyConfig {
        symbol_size: env_usize("CODEC_EFFICIENCY_SYMBOL_SIZE", 8),
        trials: env_usize("CODEC_EFFICIENCY_TRIALS", 32),
    };
    let run_mettle = codec_enabled("CODEC_EFFICIENCY_CODECS", "mettle");
    let run_raptorq = codec_enabled("CODEC_EFFICIENCY_CODECS", "raptorq");

    eprintln!(
        "codec,k,symbol_size,loss,overhead,trials,successes,avg_tx_packets,avg_rx_packets,failure_rate"
    );
    for k in ks {
        for &loss_rate in &loss_rates {
            for &overhead in &overheads {
                if run_raptorq {
                    if let Some(metrics) =
                        measure_raptorq_efficiency(k, loss_rate, overhead, config)
                    {
                        report_efficiency_row("raptorq", k, loss_rate, overhead, config, metrics);
                    } else {
                        eprintln!(
                            "raptorq,{k},{},{loss_rate:.4},{overhead:.4},{},unsupported_single_block,unsupported_single_block,unsupported_single_block,unsupported_single_block",
                            config.symbol_size, config.trials
                        );
                    }
                }
                if run_mettle {
                    report_efficiency_row(
                        "mettle",
                        k,
                        loss_rate,
                        overhead,
                        config,
                        measure_mettle_efficiency(k, loss_rate, overhead, config),
                    );
                }
            }
        }
    }
}
