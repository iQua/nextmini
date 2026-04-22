use std::collections::{HashMap, HashSet, VecDeque};
use std::num::NonZeroUsize;

use mettle::test_support::{
    Decoder as TestDecoder, Encoder as TestEncoder, edge_bin_ids_with_terminal_source_count,
};
use mettle::{MettleParams, OverheadRatio};
use raptorq::{EncodingPacket, ObjectTransmissionInformation, SourceBlockDecoder, SourceBlockEncoder};

const PAPER_CODING_EFFICIENCY_METTLE_SOURCE_COUNT: usize = 100_000;
const PAPER_CODING_EFFICIENCY_METTLE_SEED: u64 = 0;
const PAPER_CODING_EFFICIENCY_METTLE_SYMBOL_SIZE: usize = 8;
const PAPER_CODING_EFFICIENCY_RAPTORQ_SYMBOL_SIZE: usize = 1500;
const TARGET_FAILURE_RATE: f64 = 1e-3;

#[derive(Clone, Copy)]
enum MettleTrialOutcome {
    Success,
    Stalled {
        next_source_id: u64,
        stalled_run_length: u64,
        remaining_sources: u64,
    },
    PayloadMismatch {
        source_id: u64,
    },
}

#[derive(Clone, Copy)]
struct SkipProfile {
    total_skipped_sources: u64,
    max_consecutive_skip_run: u64,
}

struct MettleReplay {
    decoder: TestDecoder,
    delivered_bin_ids: HashSet<u128>,
    first_payload_mismatch: Option<u64>,
}

struct OfflinePeelingOutcome {
    first_undecoded_source_id: Option<u64>,
    undecoded_sources: usize,
    isolated_sources: usize,
    sample_undecoded_source_ids: Vec<u64>,
}

impl OfflinePeelingOutcome {
    fn summary(&self) -> String {
        match self.first_undecoded_source_id {
            Some(first_undecoded_source_id) => format!(
                "stalled:first_undecoded_source_id={} undecoded_sources={} isolated_sources={} sample_undecoded_source_ids={:?}",
                first_undecoded_source_id,
                self.undecoded_sources,
                self.isolated_sources,
                self.sample_undecoded_source_ids
            ),
            None => format!("success:isolated_sources={}", self.isolated_sources),
        }
    }

    fn is_isolated_error_floor_event(&self) -> bool {
        self.undecoded_sources == 0 && self.isolated_sources != 0
    }

    fn is_local_residual_event(&self, source_limit: usize) -> bool {
        self.undecoded_sources != 0 && self.undecoded_sources <= source_limit
    }
}

#[derive(Clone, Copy)]
struct FailureRateEstimate {
    failures: usize,
    trials: usize,
    local_residual_events: usize,
    isolated_error_floor_events: usize,
}

impl FailureRateEstimate {
    fn rate(self) -> f64 {
        self.failures as f64 / self.trials as f64
    }
}

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
struct CodingEfficiencyCase {
    name: &'static str,
    channel: Channel,
    mettle_overhead_ratio: Rational,
    raptorq_k: usize,
    raptorq_overhead_ratio: Rational,
}

const CODING_EFFICIENCY_CASES: [CodingEfficiencyCase; 10] = [
    CodingEfficiencyCase {
        name: "BEC(0.01)",
        channel: Channel::Bec {
            erasure_probability: Rational::new(1, 100),
        },
        mettle_overhead_ratio: Rational::new(550, 10_000),
        raptorq_k: 114,
        raptorq_overhead_ratio: Rational::new(614, 10_000),
    },
    CodingEfficiencyCase {
        name: "BEC(0.02)",
        channel: Channel::Bec {
            erasure_probability: Rational::new(2, 100),
        },
        mettle_overhead_ratio: Rational::new(800, 10_000),
        raptorq_k: 168,
        raptorq_overhead_ratio: Rational::new(714, 10_000),
    },
    CodingEfficiencyCase {
        name: "BEC(0.03)",
        channel: Channel::Bec {
            erasure_probability: Rational::new(3, 100),
        },
        mettle_overhead_ratio: Rational::new(900, 10_000),
        raptorq_k: 236,
        raptorq_overhead_ratio: Rational::new(763, 10_000),
    },
    CodingEfficiencyCase {
        name: "BEC(0.08)",
        channel: Channel::Bec {
            erasure_probability: Rational::new(8, 100),
        },
        mettle_overhead_ratio: Rational::new(2000, 10_000),
        raptorq_k: 269,
        raptorq_overhead_ratio: Rational::new(1560, 10_000),
    },
    CodingEfficiencyCase {
        name: "BEC(0.10)",
        channel: Channel::Bec {
            erasure_probability: Rational::new(10, 100),
        },
        mettle_overhead_ratio: Rational::new(2500, 10_000),
        raptorq_k: 405,
        raptorq_overhead_ratio: Rational::new(1500, 10_000),
    },
    CodingEfficiencyCase {
        name: "VoIP",
        channel: Channel::Ge {
            p_good_to_bad: Rational::new(5, 10_000),
            p_bad_to_good: Rational::new(1, 5),
            epsilon_good: Rational::new(1, 100),
            epsilon_bad: Rational::new(1, 1),
        },
        mettle_overhead_ratio: Rational::new(900, 10_000),
        raptorq_k: 84,
        raptorq_overhead_ratio: Rational::new(2380, 10_000),
    },
    CodingEfficiencyCase {
        name: "WiMAX",
        channel: Channel::Ge {
            p_good_to_bad: Rational::new(4, 100),
            p_bad_to_good: Rational::new(5, 100),
            epsilon_good: Rational::new(1, 100),
            epsilon_bad: Rational::new(2, 100),
        },
        mettle_overhead_ratio: Rational::new(600, 10_000),
        raptorq_k: 149,
        raptorq_overhead_ratio: Rational::new(604, 10_000),
    },
    CodingEfficiencyCase {
        name: "Video-conf-light",
        channel: Channel::Ge {
            p_good_to_bad: Rational::new(5, 100),
            p_bad_to_good: Rational::new(75, 100),
            epsilon_good: Rational::new(1, 100),
            epsilon_bad: Rational::new(1, 10),
        },
        mettle_overhead_ratio: Rational::new(800, 10_000),
        raptorq_k: 114,
        raptorq_overhead_ratio: Rational::new(702, 10_000),
    },
    CodingEfficiencyCase {
        name: "Video-conf-heavy",
        channel: Channel::Ge {
            p_good_to_bad: Rational::new(5, 100),
            p_bad_to_good: Rational::new(75, 100),
            epsilon_good: Rational::new(5, 100),
            epsilon_bad: Rational::new(1, 2),
        },
        mettle_overhead_ratio: Rational::new(2000, 10_000),
        raptorq_k: 257,
        raptorq_overhead_ratio: Rational::new(1556, 10_000),
    },
    CodingEfficiencyCase {
        name: "Long-fade",
        channel: Channel::Ge {
            p_good_to_bad: Rational::new(1, 1000),
            p_bad_to_good: Rational::new(1, 100),
            epsilon_good: Rational::new(1, 100),
            epsilon_bad: Rational::new(1, 10),
        },
        mettle_overhead_ratio: Rational::new(1200, 10_000),
        raptorq_k: 101,
        raptorq_overhead_ratio: Rational::new(1584, 10_000),
    },
];

fn raptorq_fixture_data(source_count: usize) -> Vec<u8> {
    (0..source_count)
        .flat_map(|source_id| {
            let mut payload = vec![0; PAPER_CODING_EFFICIENCY_RAPTORQ_SYMBOL_SIZE];
            payload[..std::mem::size_of::<usize>()].copy_from_slice(&source_id.to_le_bytes());
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

fn raptorq_trial_succeeds(case: CodingEfficiencyCase, seed: u64) -> bool {
    let total_packets = total_packet_count(case.raptorq_k, case.raptorq_overhead_ratio);
    let repair_packets = total_packets.saturating_sub(case.raptorq_k);
    let flat_data = raptorq_fixture_data(case.raptorq_k);
    let oti = ObjectTransmissionInformation::new(
        flat_data.len() as u64,
        PAPER_CODING_EFFICIENCY_RAPTORQ_SYMBOL_SIZE as u16,
        1,
        1,
        1,
    );
    let encoder = SourceBlockEncoder::new(0, &oti, &flat_data);
    let mut delivered_packets = Vec::<EncodingPacket>::new();
    let mut channel_state = ChannelState::new(case.channel, seed ^ 0xC0DE_CAFE_F00D_BAAD);

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

fn mettle_params(overhead_ratio: Rational) -> MettleParams {
    MettleParams::new(
        OverheadRatio::new(overhead_ratio.numerator, overhead_ratio.denominator)
            .expect("paper coding-efficiency overhead is valid"),
    )
}

fn mettle_source_payload(source_id: u64) -> [u8; PAPER_CODING_EFFICIENCY_METTLE_SYMBOL_SIZE] {
    source_id.to_le_bytes()
}

fn deliver_mettle_bin(
    decoder: &mut TestDecoder,
    delivered_bin_ids: &mut HashSet<u128>,
    channel_state: &mut ChannelState,
    first_payload_mismatch: &mut Option<u64>,
    bin_id: u128,
    payload: Vec<u8>,
) {
    if channel_state.delivers_next_packet() {
        delivered_bin_ids.insert(bin_id);
        for (source_id, decoded_payload) in decoder.push_bin(bin_id, payload) {
            let expected_payload = mettle_source_payload(source_id);
            if decoded_payload.as_slice() != expected_payload.as_slice() {
                first_payload_mismatch.get_or_insert(source_id);
            }
        }
    }
}

fn mettle_source_is_fully_erased(
    params: MettleParams,
    source_id: u64,
    graph_seed: u64,
    terminal_source_count: u64,
    delivered_bin_ids: &HashSet<u128>,
) -> bool {
    edge_bin_ids_with_terminal_source_count(
        params,
        source_id,
        graph_seed,
        Some(terminal_source_count),
    )
        .into_iter()
        .all(|bin_id| !delivered_bin_ids.contains(&bin_id))
}

fn offline_peeling_outcome(
    params: MettleParams,
    graph_seed: u64,
    source_count: usize,
    delivered_bin_ids: &HashSet<u128>,
) -> OfflinePeelingOutcome {
    let terminal_source_count = source_count as u64;
    let mut source_edges = Vec::with_capacity(source_count);
    let mut bin_touchers = HashMap::<u128, Vec<usize>>::new();

    for source_id in 0..terminal_source_count {
        let mut delivered_edges = Vec::<u128>::with_capacity(MettleParams::EDGE_COUNT);
        for bin_id in edge_bin_ids_with_terminal_source_count(
            params,
            source_id,
            graph_seed,
            Some(terminal_source_count),
        ) {
            if delivered_bin_ids.contains(&bin_id) && !delivered_edges.contains(&bin_id) {
                delivered_edges.push(bin_id);
                bin_touchers.entry(bin_id).or_default().push(source_id as usize);
            }
        }
        source_edges.push(delivered_edges);
    }

    let mut remaining_touchers = bin_touchers
        .iter()
        .map(|(&bin_id, touchers)| (bin_id, touchers.len()))
        .collect::<HashMap<_, _>>();
    let mut queue = remaining_touchers
        .iter()
        .filter_map(|(&bin_id, &count)| (count == 1).then_some(bin_id))
        .collect::<VecDeque<_>>();
    let mut decoded = vec![false; source_count];

    while let Some(bin_id) = queue.pop_front() {
        if remaining_touchers.get(&bin_id).copied() != Some(1) {
            continue;
        }
        let Some(source_id) = bin_touchers
            .get(&bin_id)
            .and_then(|touchers| touchers.iter().copied().find(|&source_id| !decoded[source_id]))
        else {
            continue;
        };
        decoded[source_id] = true;
        for &edge_bin_id in &source_edges[source_id] {
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

    let mut first_undecoded_source_id = None;
    let mut undecoded_sources = 0usize;
    let mut isolated_sources = 0usize;
    let mut sample_undecoded_source_ids = Vec::new();

    for (source_id, (is_decoded, edges)) in decoded.iter().zip(source_edges.iter()).enumerate() {
        if *is_decoded {
            continue;
        }
        if edges.is_empty() {
            isolated_sources += 1;
            continue;
        }
        first_undecoded_source_id.get_or_insert(source_id as u64);
        undecoded_sources += 1;
        if sample_undecoded_source_ids.len() < 8 {
            sample_undecoded_source_ids.push(source_id as u64);
        }
    }

    OfflinePeelingOutcome {
        first_undecoded_source_id,
        undecoded_sources,
        isolated_sources,
        sample_undecoded_source_ids,
    }
}

fn format_source_edges(
    params: MettleParams,
    graph_seed: u64,
    terminal_source_count: u64,
    delivered_bin_ids: &HashSet<u128>,
    source_ids: &[u64],
) -> String {
    source_ids
        .iter()
        .map(|&source_id| {
            let edges = edge_bin_ids_with_terminal_source_count(
                params,
                source_id,
                graph_seed,
                Some(terminal_source_count),
            )
            .into_iter()
            .map(|bin_id| {
                format!(
                    "{}:delivered={}",
                    bin_id,
                    delivered_bin_ids.contains(&bin_id)
                )
            })
            .collect::<Vec<_>>()
            .join("|");
            format!("{}=[{}]", source_id, edges)
        })
        .collect::<Vec<_>>()
        .join(", ")
}

fn isolated_error_floor_run_length(
    params: MettleParams,
    first_source_id: u64,
    graph_seed: u64,
    terminal_source_count: u64,
    delivered_bin_ids: &HashSet<u128>,
) -> u64 {
    let mut run_length = 0;

    for source_id in first_source_id..terminal_source_count {
        if !mettle_source_is_fully_erased(
            params,
            source_id,
            graph_seed,
            terminal_source_count,
            delivered_bin_ids,
        ) {
            break;
        }
        run_length += 1;
    }

    run_length
}

fn mettle_trial_outcome(
    case: CodingEfficiencyCase,
    seed: u64,
    source_count: usize,
) -> MettleTrialOutcome {
    let params = case_params(case);
    let terminal_source_count = source_count as u64;
    let graph_seed = mettle_graph_seed(seed);
    let MettleReplay {
        mut decoder,
        delivered_bin_ids,
        first_payload_mismatch,
    } = replay_mettle_trial(case, seed, source_count);

    if let Some(source_id) = first_payload_mismatch {
        return MettleTrialOutcome::PayloadMismatch { source_id };
    }

    loop {
        let next_source_id = decoder.next_source_id();
        if next_source_id == terminal_source_count {
            return MettleTrialOutcome::Success;
        }
        let stalled_run_length = isolated_error_floor_run_length(
            params,
            next_source_id,
            graph_seed,
            terminal_source_count,
            &delivered_bin_ids,
        );
        if stalled_run_length == 0 {
            return MettleTrialOutcome::Stalled {
                next_source_id,
                stalled_run_length,
                remaining_sources: terminal_source_count - next_source_id,
            };
        }
        for _ in 0..stalled_run_length {
            let _ = decoder.skip_next_source_without_edges();
        }
    }
}

fn mettle_trial_succeeds(case: CodingEfficiencyCase, seed: u64, source_count: usize) -> bool {
    matches!(
        mettle_trial_outcome(case, seed, source_count),
        MettleTrialOutcome::Success
    )
}

fn replay_mettle_trial(
    case: CodingEfficiencyCase,
    seed: u64,
    source_count: usize,
) -> MettleReplay {
    let params = case_params(case);
    let source_symbol_bytes =
        NonZeroUsize::new(PAPER_CODING_EFFICIENCY_METTLE_SYMBOL_SIZE).expect("non-zero symbol size");
    let terminal_source_count = source_count as u64;
    let graph_seed = mettle_graph_seed(seed);
    let mut encoder = TestEncoder::new_terminated(
        params,
        source_symbol_bytes,
        graph_seed,
        terminal_source_count,
    );
    let mut decoder = TestDecoder::new_terminated(
        params,
        source_symbol_bytes,
        graph_seed,
        terminal_source_count,
    );
    let mut delivered_bin_ids = HashSet::new();
    let mut first_payload_mismatch = None;
    let mut channel_state = ChannelState::new(case.channel, seed ^ 0xC0DE_CAFE_F00D_BAAD);

    for source_id in 0..terminal_source_count {
        for (bin_id, payload) in encoder.push_source(&mettle_source_payload(source_id)) {
            deliver_mettle_bin(
                &mut decoder,
                &mut delivered_bin_ids,
                &mut channel_state,
                &mut first_payload_mismatch,
                bin_id,
                payload,
            );
        }
    }
    for (bin_id, payload) in encoder.finish() {
        deliver_mettle_bin(
            &mut decoder,
            &mut delivered_bin_ids,
            &mut channel_state,
            &mut first_payload_mismatch,
            bin_id,
            payload,
        );
    }

    MettleReplay {
        decoder,
        delivered_bin_ids,
        first_payload_mismatch,
    }
}

fn skip_profile_after_replay(case: CodingEfficiencyCase, seed: u64, source_count: usize) -> SkipProfile {
    let terminal_source_count = source_count as u64;
    let MettleReplay { mut decoder, .. } = replay_mettle_trial(case, seed, source_count);
    let mut total_skipped_sources = 0;
    let mut current_skip_run = 0;
    let mut max_consecutive_skip_run = 0;

    while decoder.next_source_id() < terminal_source_count {
        let before = decoder.next_source_id();
        let _ = decoder.skip_next_source_without_edges();
        let after = decoder.next_source_id();
        total_skipped_sources += 1;
        current_skip_run += 1;
        if after > before + 1 {
            max_consecutive_skip_run = max_consecutive_skip_run.max(current_skip_run);
            current_skip_run = 0;
        }
    }
    max_consecutive_skip_run = max_consecutive_skip_run.max(current_skip_run);

    SkipProfile {
        total_skipped_sources,
        max_consecutive_skip_run,
    }
}

fn raptorq_estimated_failure_rate(case: CodingEfficiencyCase, trials: usize) -> FailureRateEstimate {
    let failures = (0..trials)
        .filter(|&trial| !raptorq_trial_succeeds(case, trial as u64 + 1))
        .count();

    FailureRateEstimate {
        failures,
        trials,
        local_residual_events: 0,
        isolated_error_floor_events: 0,
    }
}

fn mettle_estimated_failure_rate(
    case: CodingEfficiencyCase,
    trials: usize,
) -> FailureRateEstimate {
    let print_first_failure = std::env::var("METTLE_TABLE_IV_PRINT_FIRST_FAILURE")
        .ok()
        .is_some_and(|value| value != "0");
    let source_count = mettle_table_iv_source_count();
    let local_residual_source_limit = mettle_table_iv_local_residual_source_limit();
    let params = case_params(case);
    let mut failures = 0usize;
    let mut local_residual_events = 0usize;
    let mut isolated_error_floor_events = 0usize;

    for trial in 0..trials {
        match mettle_trial_outcome(case, trial as u64 + 1, source_count) {
            MettleTrialOutcome::Success => {}
            MettleTrialOutcome::Stalled {
                next_source_id,
                stalled_run_length,
                remaining_sources,
            } => {
                let MettleReplay {
                    decoder,
                    delivered_bin_ids,
                    ..
                } = replay_mettle_trial(case, trial as u64 + 1, source_count);
                let offline_outcome = offline_peeling_outcome(
                    params,
                    mettle_graph_seed(trial as u64 + 1),
                    source_count,
                    &delivered_bin_ids,
                );
                if offline_outcome.is_isolated_error_floor_event() {
                    isolated_error_floor_events += 1;
                    if print_first_failure && isolated_error_floor_events == 1 {
                        let offline_sample_edges = format_source_edges(
                            params,
                            mettle_graph_seed(trial as u64 + 1),
                            source_count as u64,
                            &delivered_bin_ids,
                            &offline_outcome.sample_undecoded_source_ids,
                        );
                        eprintln!(
                            "first_mettle_isolated_error_floor channel={} trial={} next_source_id={} stalled_run_length={} remaining_sources={} offline_peeling={} offline_sample_edges=[{}]",
                            case.name,
                            trial + 1,
                            next_source_id,
                            stalled_run_length,
                            remaining_sources,
                            offline_outcome.summary(),
                            offline_sample_edges,
                        );
                    }
                    continue;
                }
                if offline_outcome.is_local_residual_event(local_residual_source_limit) {
                    local_residual_events += 1;
                    if print_first_failure && local_residual_events == 1 {
                        let offline_sample_edges = format_source_edges(
                            params,
                            mettle_graph_seed(trial as u64 + 1),
                            source_count as u64,
                            &delivered_bin_ids,
                            &offline_outcome.sample_undecoded_source_ids,
                        );
                        eprintln!(
                            "first_mettle_local_residual channel={} trial={} next_source_id={} stalled_run_length={} remaining_sources={} local_residual_source_limit={} offline_peeling={} offline_sample_edges=[{}]",
                            case.name,
                            trial + 1,
                            next_source_id,
                            stalled_run_length,
                            remaining_sources,
                            local_residual_source_limit,
                            offline_outcome.summary(),
                            offline_sample_edges,
                        );
                    }
                    continue;
                }

                failures += 1;
                if print_first_failure && failures == 1 {
                    let skip_profile = skip_profile_after_replay(case, trial as u64 + 1, source_count);
                    let edge_bin_ids = edge_bin_ids_with_terminal_source_count(
                        params,
                        next_source_id,
                        mettle_graph_seed(trial as u64 + 1),
                        Some(source_count as u64),
                    );
                    let edge_details = edge_bin_ids
                        .into_iter()
                        .map(|bin_id| {
                            format!(
                                "{}:delivered={}:remaining={:?}",
                                bin_id,
                                delivered_bin_ids.contains(&bin_id),
                                decoder.buffered_bin_remaining_touchers(bin_id)
                            )
                        })
                        .collect::<Vec<_>>()
                        .join(", ");
                    let offline_sample_edges = format_source_edges(
                        params,
                        mettle_graph_seed(trial as u64 + 1),
                        source_count as u64,
                        &delivered_bin_ids,
                        &offline_outcome.sample_undecoded_source_ids,
                    );
                    eprintln!(
                        "first_mettle_failure channel={} trial={} next_source_id={} stalled_run_length={} remaining_sources={} skip_total={} skip_max_run={} offline_peeling={} offline_sample_edges=[{}] edges=[{}]",
                        case.name,
                        trial + 1,
                        next_source_id,
                        stalled_run_length,
                        remaining_sources,
                        skip_profile.total_skipped_sources,
                        skip_profile.max_consecutive_skip_run,
                        offline_outcome.summary(),
                        offline_sample_edges,
                        edge_details,
                    );
                }
            }
            MettleTrialOutcome::PayloadMismatch { source_id } => {
                failures += 1;
                if print_first_failure && failures == 1 {
                    eprintln!(
                        "first_mettle_failure channel={} trial={} payload_mismatch_source_id={}",
                        case.name,
                        trial + 1,
                        source_id,
                    );
                }
            }
        }
    }

    FailureRateEstimate {
        failures,
        trials,
        local_residual_events,
        isolated_error_floor_events,
    }
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

fn div_ceil(lhs: usize, rhs: usize) -> usize {
    lhs / rhs + (!lhs.is_multiple_of(rhs)) as usize
}

fn case_params(case: CodingEfficiencyCase) -> MettleParams {
    mettle_params(case.mettle_overhead_ratio)
}

fn mettle_table_iv_source_count() -> usize {
    std::env::var("METTLE_TABLE_IV_SOURCE_COUNT")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(PAPER_CODING_EFFICIENCY_METTLE_SOURCE_COUNT)
}

fn mettle_table_iv_local_residual_source_limit() -> usize {
    std::env::var("METTLE_TABLE_IV_LOCAL_RESIDUAL_SOURCE_LIMIT")
        .or_else(|_| std::env::var("METTLE_TABLE_IV_LOCAL_ERROR_FLOOR_SOURCE_LIMIT"))
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(0)
}

fn mettle_graph_seed(trial_seed: u64) -> u64 {
    match std::env::var("METTLE_TABLE_IV_GRAPH_SEED_MODE").as_deref() {
        Ok("fixed") => PAPER_CODING_EFFICIENCY_METTLE_SEED,
        _ => PAPER_CODING_EFFICIENCY_METTLE_SEED ^ trial_seed,
    }
}

#[test]
fn paper_coding_efficiency_raptorq_harness_decodes_a_small_bec_case() {
    let case = CODING_EFFICIENCY_CASES[0];

    assert!(raptorq_trial_succeeds(case, 1));
}

#[test]
fn paper_coding_efficiency_mettle_harness_decodes_a_small_bec_case() {
    let case = CodingEfficiencyCase {
        name: "smoke-no-loss",
        channel: Channel::Bec {
            erasure_probability: Rational::new(0, 1),
        },
        mettle_overhead_ratio: Rational::new(550, 10_000),
        raptorq_k: 114,
        raptorq_overhead_ratio: Rational::new(614, 10_000),
    };

    assert!(mettle_trial_succeeds(case, 1, 512));
}

#[test]
#[ignore = "manual paper coding-efficiency reproduction"]
fn report_paper_coding_efficiency_failure_rates() {
    let trials = std::env::var("METTLE_TABLE_IV_TRIALS")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(4096);
    let name_filter = std::env::var("METTLE_TABLE_IV_FILTER").ok();

    for case in CODING_EFFICIENCY_CASES {
        if let Some(filter) = &name_filter
            && !case.name.contains(filter)
        {
            continue;
        }
        let mettle_estimate = mettle_estimated_failure_rate(case, trials);
        let raptorq_estimate = raptorq_estimated_failure_rate(case, trials);
        eprintln!(
            "channel={} mettle_overhead={:.4}% mettle_stall_failures={}/{} mettle_stall_failure_rate={:.6} mettle_local_residual_events={}/{} mettle_isolated_error_floor_events={}/{} raptorq_k={} raptorq_overhead={:.4}% raptorq_failures={}/{} raptorq_failure_rate={:.6} target={:.6}",
            case.name,
            case.mettle_overhead_ratio.to_f64() * 100.0,
            mettle_estimate.failures,
            mettle_estimate.trials,
            mettle_estimate.rate(),
            mettle_estimate.local_residual_events,
            mettle_estimate.trials,
            mettle_estimate.isolated_error_floor_events,
            mettle_estimate.trials,
            case.raptorq_k,
            case.raptorq_overhead_ratio.to_f64() * 100.0,
            raptorq_estimate.failures,
            raptorq_estimate.trials,
            raptorq_estimate.rate(),
            TARGET_FAILURE_RATE,
        );
    }
}
