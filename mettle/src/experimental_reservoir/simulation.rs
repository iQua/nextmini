//! Deterministic graph-only simulation for an extension **BEYOND the METTLE paper**.
//!
//! This simulator uses the production finite edge generator and exact terminal counts, but peels
//! equation degrees without allocating symbol payloads. A comparison test pins its completion
//! result to the real encoder/decoder. It models only first global reserve emissions; a lost
//! already-emitted reserve bin remains Stage 2.4 retransmission traffic and is intentionally not
//! relabeled fresh here.

use std::collections::VecDeque;

use crate::MettleParams;

use super::{
    FiniteReservoirGeometry, FreshReserveEmitter, ReserveSet, ReservoirError, ReservoirRate,
    splitmix64,
};

const GRAPH_SEED_DOMAIN: u64 = 0x4752_4150_485F_5333;
const RESERVE_SEED_DOMAIN: u64 = 0x5253_565F_5052_4653;
const CHANNEL_SEED_DOMAIN: u64 = 0x4348_414E_4E45_4C33;

/// A memoryless or Gilbert-Elliott loss trace used by the Stage 3.1 sweep.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ChannelModel {
    Bec {
        name: &'static str,
        erasure: ReservoirRate,
    },
    GilbertElliott {
        name: &'static str,
        good_to_bad: ReservoirRate,
        bad_to_good: ReservoirRate,
        erasure_good: ReservoirRate,
        erasure_bad: ReservoirRate,
    },
}

impl ChannelModel {
    pub fn bec(name: &'static str, erasure: ReservoirRate) -> Result<Self, SimulationError> {
        validate_probability(erasure)?;
        Ok(Self::Bec { name, erasure })
    }

    pub fn gilbert_elliott(
        name: &'static str,
        good_to_bad: ReservoirRate,
        bad_to_good: ReservoirRate,
        erasure_good: ReservoirRate,
        erasure_bad: ReservoirRate,
    ) -> Result<Self, SimulationError> {
        for probability in [good_to_bad, bad_to_good, erasure_good, erasure_bad] {
            validate_probability(probability)?;
        }
        if good_to_bad.numerator() == 0 && bad_to_good.numerator() == 0 {
            return Err(SimulationError::DegenerateGilbertElliottChain);
        }
        Ok(Self::GilbertElliott {
            name,
            good_to_bad,
            bad_to_good,
            erasure_good,
            erasure_bad,
        })
    }

    #[must_use]
    pub const fn name(self) -> &'static str {
        match self {
            Self::Bec { name, .. } | Self::GilbertElliott { name, .. } => name,
        }
    }

    #[must_use]
    pub fn stationary_erasure_probability(self) -> f64 {
        match self {
            Self::Bec { erasure, .. } => erasure.as_f64(),
            Self::GilbertElliott {
                good_to_bad,
                bad_to_good,
                erasure_good,
                erasure_bad,
                ..
            } => {
                let bad_fraction =
                    good_to_bad.as_f64() / (good_to_bad.as_f64() + bad_to_good.as_f64());
                (1.0 - bad_fraction) * erasure_good.as_f64() + bad_fraction * erasure_bad.as_f64()
            }
        }
    }
}

/// Exact two-sided Clopper-Pearson interval for a binomial completion probability.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct ClopperPearsonInterval {
    pub lower: f64,
    pub upper: f64,
}

/// Aggregate metrics for one `(c_wire, c_reserve, channel)` case.
#[derive(Clone, Debug, PartialEq)]
pub struct SimulationSummary {
    pub trials: usize,
    pub initial_completions: usize,
    pub final_completions: usize,
    pub completion_interval_95: ClopperPearsonInterval,
    pub repair_successes: usize,
    pub mean_repair_emissions_for_success: f64,
    pub p95_repair_emissions_for_success: usize,
    pub mean_reserve_emissions: f64,
    pub mean_reserve_losses: f64,
    pub duplicate_transmissions: u64,
}

impl SimulationSummary {
    #[must_use]
    pub fn initial_completion_probability(&self) -> f64 {
        self.initial_completions as f64 / self.trials as f64
    }

    #[must_use]
    pub fn completion_probability(&self) -> f64 {
        self.final_completions as f64 / self.trials as f64
    }
}

/// Run independent graph/channel trials for one finite geometry.
pub fn run_case(
    geometry: FiniteReservoirGeometry,
    channel: ChannelModel,
    trials: usize,
    seed: u64,
) -> Result<SimulationSummary, SimulationError> {
    if trials == 0 {
        return Err(SimulationError::ZeroTrials);
    }

    let mut initial_completions = 0usize;
    let mut final_completions = 0usize;
    let mut repair_successes = 0usize;
    let mut successful_repair_emissions = Vec::new();
    successful_repair_emissions
        .try_reserve(trials)
        .map_err(|_| SimulationError::AllocationFailed)?;
    let mut reserve_emissions = 0u128;
    let mut reserve_losses = 0u128;
    let mut duplicate_transmissions = 0u64;

    for trial in 0..trials {
        let trial = u64::try_from(trial).map_err(|_| SimulationError::ArithmeticOverflow)?;
        let graph_seed = splitmix64(seed ^ GRAPH_SEED_DOMAIN ^ trial);
        let reserve_seed = splitmix64(seed ^ RESERVE_SEED_DOMAIN ^ trial);
        let channel_seed = splitmix64(seed ^ CHANNEL_SEED_DOMAIN ^ trial);
        let graph = FiniteGraph::build(geometry, graph_seed)?;
        let reserve = ReserveSet::select(geometry, reserve_seed)?;
        let outcome = run_trial(&graph, &reserve, channel, channel_seed)?;

        initial_completions += usize::from(outcome.initial_complete);
        final_completions += usize::from(outcome.final_complete);
        reserve_emissions = reserve_emissions
            .checked_add(outcome.reserve_emissions as u128)
            .ok_or(SimulationError::ArithmeticOverflow)?;
        reserve_losses = reserve_losses
            .checked_add(outcome.reserve_losses as u128)
            .ok_or(SimulationError::ArithmeticOverflow)?;
        duplicate_transmissions = duplicate_transmissions
            .checked_add(outcome.duplicate_transmissions)
            .ok_or(SimulationError::ArithmeticOverflow)?;
        if !outcome.initial_complete && outcome.final_complete {
            repair_successes += 1;
            successful_repair_emissions.push(outcome.reserve_emissions);
        }
    }

    successful_repair_emissions.sort_unstable();
    let mean_repair_emissions_for_success = if successful_repair_emissions.is_empty() {
        0.0
    } else {
        successful_repair_emissions.iter().sum::<usize>() as f64
            / successful_repair_emissions.len() as f64
    };

    Ok(SimulationSummary {
        trials,
        initial_completions,
        final_completions,
        completion_interval_95: clopper_pearson(final_completions, trials, 0.95)?,
        repair_successes,
        mean_repair_emissions_for_success,
        p95_repair_emissions_for_success: percentile_ceil(&successful_repair_emissions, 95, 100),
        mean_reserve_emissions: reserve_emissions as f64 / trials as f64,
        mean_reserve_losses: reserve_losses as f64 / trials as f64,
        duplicate_transmissions,
    })
}

/// Compute a two-sided exact binomial interval by inverting binomial tails.
pub fn clopper_pearson(
    successes: usize,
    trials: usize,
    confidence: f64,
) -> Result<ClopperPearsonInterval, SimulationError> {
    if trials == 0 || successes > trials || !(0.0..1.0).contains(&confidence) {
        return Err(SimulationError::InvalidConfidenceInputs);
    }
    let tail = (1.0 - confidence) / 2.0;
    let lower = if successes == 0 {
        0.0
    } else {
        invert_increasing_tail(0.0, successes as f64 / trials as f64, tail, |probability| {
            binomial_cdf(trials - successes, trials, 1.0 - probability)
        })
    };
    let upper = if successes == trials {
        1.0
    } else {
        invert_decreasing_tail(successes as f64 / trials as f64, 1.0, tail, |probability| {
            binomial_cdf(successes, trials, probability)
        })
    };

    Ok(ClopperPearsonInterval { lower, upper })
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SimulationError {
    Reservoir(ReservoirError),
    InvalidProbability,
    DegenerateGilbertElliottChain,
    ZeroTrials,
    InvalidConfidenceInputs,
    ArithmeticOverflow,
    GeometryTooLarge,
    AllocationFailed,
    GraphInvariantViolated,
}

impl From<ReservoirError> for SimulationError {
    fn from(error: ReservoirError) -> Self {
        Self::Reservoir(error)
    }
}

#[derive(Clone, Copy)]
struct SourceEdges {
    bin_ids: [u32; MettleParams::EDGE_COUNT],
    len: u8,
}

impl SourceEdges {
    fn iter(self) -> impl Iterator<Item = u32> {
        self.bin_ids.into_iter().take(usize::from(self.len))
    }
}

struct FiniteGraph {
    source_edges: Vec<SourceEdges>,
    bin_sources: Vec<Vec<u32>>,
}

impl FiniteGraph {
    fn build(geometry: FiniteReservoirGeometry, graph_seed: u64) -> Result<Self, SimulationError> {
        let source_count = usize::try_from(geometry.source_count())
            .map_err(|_| SimulationError::GeometryTooLarge)?;
        let bin_count = usize::try_from(geometry.terminal_bin_count())
            .map_err(|_| SimulationError::GeometryTooLarge)?;
        let mut source_edges = Vec::new();
        source_edges
            .try_reserve_exact(source_count)
            .map_err(|_| SimulationError::AllocationFailed)?;
        let mut bin_sources = Vec::new();
        bin_sources
            .try_reserve_exact(bin_count)
            .map_err(|_| SimulationError::AllocationFailed)?;
        bin_sources.resize_with(bin_count, Vec::new);

        for source_id in 0..geometry.source_count() {
            let mut bin_ids = geometry.params().edge_bin_ids_with_terminal_source_count(
                source_id,
                graph_seed,
                Some(geometry.source_count()),
            );
            bin_ids.sort_unstable();
            let mut unique = [0u32; MettleParams::EDGE_COUNT];
            let mut unique_len = 0usize;
            for bin_id in bin_ids {
                if unique_len != 0 && u128::from(unique[unique_len - 1]) == bin_id {
                    continue;
                }
                let bin_id =
                    u32::try_from(bin_id).map_err(|_| SimulationError::GeometryTooLarge)?;
                if bin_id as usize >= bin_count {
                    return Err(SimulationError::GraphInvariantViolated);
                }
                unique[unique_len] = bin_id;
                unique_len += 1;
                bin_sources[bin_id as usize]
                    .try_reserve(1)
                    .map_err(|_| SimulationError::AllocationFailed)?;
                bin_sources[bin_id as usize]
                    .push(u32::try_from(source_id).map_err(|_| SimulationError::GeometryTooLarge)?);
            }
            source_edges.push(SourceEdges {
                bin_ids: unique,
                len: u8::try_from(unique_len).map_err(|_| SimulationError::GeometryTooLarge)?,
            });
        }

        Ok(Self {
            source_edges,
            bin_sources,
        })
    }
}

struct Peeler<'a> {
    graph: &'a FiniteGraph,
    delivered_bins: Vec<bool>,
    decoded_sources: Vec<bool>,
    remaining_degrees: Vec<usize>,
    ready_bins: VecDeque<u32>,
    decoded_count: usize,
    duplicate_transmissions: u64,
}

impl<'a> Peeler<'a> {
    fn new(graph: &'a FiniteGraph) -> Result<Self, SimulationError> {
        Ok(Self {
            graph,
            delivered_bins: try_bool_vec(graph.bin_sources.len())?,
            decoded_sources: try_bool_vec(graph.source_edges.len())?,
            remaining_degrees: try_usize_vec(graph.bin_sources.len())?,
            ready_bins: VecDeque::new(),
            decoded_count: 0,
            duplicate_transmissions: 0,
        })
    }

    fn push_bin(&mut self, bin_id: u32) -> Result<(), SimulationError> {
        let index = bin_id as usize;
        let delivered = self
            .delivered_bins
            .get_mut(index)
            .ok_or(SimulationError::GraphInvariantViolated)?;
        if *delivered {
            self.duplicate_transmissions = self
                .duplicate_transmissions
                .checked_add(1)
                .ok_or(SimulationError::ArithmeticOverflow)?;
            return Ok(());
        }
        *delivered = true;
        let degree = self.graph.bin_sources[index]
            .iter()
            .filter(|&&source_id| !self.decoded_sources[source_id as usize])
            .count();
        self.remaining_degrees[index] = degree;
        if degree == 1 {
            self.ready_bins.push_back(bin_id);
        }
        self.peel()
    }

    fn peel(&mut self) -> Result<(), SimulationError> {
        while let Some(bin_id) = self.ready_bins.pop_front() {
            let bin_index = bin_id as usize;
            if !self.delivered_bins[bin_index] || self.remaining_degrees[bin_index] != 1 {
                continue;
            }
            let Some(source_id) = self.graph.bin_sources[bin_index]
                .iter()
                .copied()
                .find(|&source_id| !self.decoded_sources[source_id as usize])
            else {
                return Err(SimulationError::GraphInvariantViolated);
            };
            self.decoded_sources[source_id as usize] = true;
            self.decoded_count += 1;

            for adjacent_bin in self.graph.source_edges[source_id as usize].iter() {
                let adjacent_index = adjacent_bin as usize;
                if !self.delivered_bins[adjacent_index] {
                    continue;
                }
                self.remaining_degrees[adjacent_index] = self.remaining_degrees[adjacent_index]
                    .checked_sub(1)
                    .ok_or(SimulationError::GraphInvariantViolated)?;
                if self.remaining_degrees[adjacent_index] == 1 {
                    self.ready_bins.push_back(adjacent_bin);
                }
            }
        }
        Ok(())
    }

    fn complete(&self) -> bool {
        self.decoded_count == self.graph.source_edges.len()
    }
}

#[derive(Clone, Copy)]
struct TrialOutcome {
    initial_complete: bool,
    final_complete: bool,
    reserve_emissions: usize,
    reserve_losses: usize,
    duplicate_transmissions: u64,
}

fn run_trial(
    graph: &FiniteGraph,
    reserve: &ReserveSet,
    channel: ChannelModel,
    channel_seed: u64,
) -> Result<TrialOutcome, SimulationError> {
    let mut reserved = try_bool_vec(graph.bin_sources.len())?;
    for &bin_id in reserve.ids_ascending() {
        let bin_id = usize::try_from(bin_id).map_err(|_| SimulationError::GeometryTooLarge)?;
        *reserved
            .get_mut(bin_id)
            .ok_or(SimulationError::GraphInvariantViolated)? = true;
    }

    let mut channel = ChannelState::new(channel, channel_seed);
    let mut peeler = Peeler::new(graph)?;
    for (bin_id, &is_reserved) in reserved.iter().enumerate() {
        if !is_reserved && channel.delivers() {
            peeler
                .push_bin(u32::try_from(bin_id).map_err(|_| SimulationError::GeometryTooLarge)?)?;
        }
    }
    let initial_complete = peeler.complete();

    let mut reserve_emissions = 0usize;
    let mut reserve_losses = 0usize;
    if !initial_complete {
        let mut emitter = FreshReserveEmitter::new(reserve);
        for &candidate in reserve.emission_order() {
            let fresh = emitter.claim_fresh([candidate], 1);
            let Some(bin_id) = fresh.into_iter().next() else {
                return Err(SimulationError::GraphInvariantViolated);
            };
            reserve_emissions += 1;
            if channel.delivers() {
                peeler.push_bin(
                    u32::try_from(bin_id).map_err(|_| SimulationError::GeometryTooLarge)?,
                )?;
                if peeler.complete() {
                    break;
                }
            } else {
                reserve_losses += 1;
            }
        }
        if emitter.emitted_count() != reserve_emissions {
            return Err(SimulationError::GraphInvariantViolated);
        }
    }

    Ok(TrialOutcome {
        initial_complete,
        final_complete: peeler.complete(),
        reserve_emissions,
        reserve_losses,
        duplicate_transmissions: peeler.duplicate_transmissions,
    })
}

struct ChannelState {
    model: ChannelModel,
    rng: SplitMix64,
    in_bad_state: bool,
}

impl ChannelState {
    fn new(model: ChannelModel, seed: u64) -> Self {
        let mut rng = SplitMix64::new(seed);
        let in_bad_state = match model {
            ChannelModel::Bec { .. } => false,
            ChannelModel::GilbertElliott {
                good_to_bad,
                bad_to_good,
                ..
            } => rng.sample_ratio(
                u64::from(good_to_bad.numerator()) * u64::from(bad_to_good.denominator()),
                u64::from(good_to_bad.numerator()) * u64::from(bad_to_good.denominator())
                    + u64::from(bad_to_good.numerator()) * u64::from(good_to_bad.denominator()),
            ),
        };
        Self {
            model,
            rng,
            in_bad_state,
        }
    }

    fn delivers(&mut self) -> bool {
        match self.model {
            ChannelModel::Bec { erasure, .. } => !self.rng.sample_rate(erasure),
            ChannelModel::GilbertElliott {
                good_to_bad,
                bad_to_good,
                erasure_good,
                erasure_bad,
                ..
            } => {
                let erased = self.rng.sample_rate(if self.in_bad_state {
                    erasure_bad
                } else {
                    erasure_good
                });
                let transition = self.rng.sample_rate(if self.in_bad_state {
                    bad_to_good
                } else {
                    good_to_bad
                });
                if transition {
                    self.in_bad_state = !self.in_bad_state;
                }
                !erased
            }
        }
    }
}

struct SplitMix64 {
    state: u64,
}

impl SplitMix64 {
    const fn new(seed: u64) -> Self {
        Self { state: seed }
    }

    fn next_u64(&mut self) -> u64 {
        self.state = self.state.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut value = self.state;
        value = (value ^ (value >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        value = (value ^ (value >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        value ^ (value >> 31)
    }

    fn sample_rate(&mut self, probability: ReservoirRate) -> bool {
        self.sample_ratio(
            u64::from(probability.numerator()),
            u64::from(probability.denominator()),
        )
    }

    fn sample_ratio(&mut self, numerator: u64, denominator: u64) -> bool {
        debug_assert!(numerator <= denominator);
        if numerator == 0 {
            return false;
        }
        if numerator == denominator {
            return true;
        }
        ((u128::from(self.next_u64()) * u128::from(denominator)) >> u64::BITS)
            < u128::from(numerator)
    }
}

fn validate_probability(probability: ReservoirRate) -> Result<(), SimulationError> {
    if probability.numerator() > probability.denominator() {
        return Err(SimulationError::InvalidProbability);
    }
    Ok(())
}

fn try_bool_vec(len: usize) -> Result<Vec<bool>, SimulationError> {
    let mut values = Vec::new();
    values
        .try_reserve_exact(len)
        .map_err(|_| SimulationError::AllocationFailed)?;
    values.resize(len, false);
    Ok(values)
}

fn try_usize_vec(len: usize) -> Result<Vec<usize>, SimulationError> {
    let mut values = Vec::new();
    values
        .try_reserve_exact(len)
        .map_err(|_| SimulationError::AllocationFailed)?;
    values.resize(len, 0);
    Ok(values)
}

fn percentile_ceil(sorted: &[usize], numerator: usize, denominator: usize) -> usize {
    if sorted.is_empty() {
        return 0;
    }
    let rank = sorted.len().saturating_mul(numerator).div_ceil(denominator);
    sorted[rank.saturating_sub(1).min(sorted.len() - 1)]
}

fn invert_increasing_tail(
    mut low: f64,
    mut high: f64,
    target: f64,
    tail: impl Fn(f64) -> f64,
) -> f64 {
    for _ in 0..64 {
        let middle = (low + high) / 2.0;
        if tail(middle) < target {
            low = middle;
        } else {
            high = middle;
        }
    }
    high
}

fn invert_decreasing_tail(
    mut low: f64,
    mut high: f64,
    target: f64,
    tail: impl Fn(f64) -> f64,
) -> f64 {
    for _ in 0..64 {
        let middle = (low + high) / 2.0;
        if tail(middle) > target {
            low = middle;
        } else {
            high = middle;
        }
    }
    high
}

fn binomial_cdf(k: usize, trials: usize, probability: f64) -> f64 {
    if k >= trials || probability == 0.0 {
        return 1.0;
    }
    if probability == 1.0 {
        return 0.0;
    }

    let log_p = probability.ln();
    let log_q = (-probability).ln_1p();
    let mut log_coefficient = 0.0;
    let mut log_sum = f64::NEG_INFINITY;
    for successes in 0..=k {
        if successes != 0 {
            log_coefficient += ((trials - successes + 1) as f64).ln() - (successes as f64).ln();
        }
        let log_probability =
            log_coefficient + successes as f64 * log_p + (trials - successes) as f64 * log_q;
        log_sum = log_add_exp(log_sum, log_probability);
    }
    log_sum.exp().clamp(0.0, 1.0)
}

fn log_add_exp(lhs: f64, rhs: f64) -> f64 {
    if lhs.is_infinite() && lhs.is_sign_negative() {
        return rhs;
    }
    let maximum = lhs.max(rhs);
    maximum + ((lhs - maximum).exp() + (rhs - maximum).exp()).ln()
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroUsize;

    use crate::test_support::{Decoder, Encoder};

    use super::*;

    fn rate(numerator: u32, denominator: u32) -> ReservoirRate {
        ReservoirRate::new(numerator, denominator).expect("test rate is valid")
    }

    #[test]
    fn clopper_pearson_matches_known_exact_interval() {
        let interval = clopper_pearson(5, 10, 0.95).expect("valid interval");
        assert!((interval.lower - 0.187_086).abs() < 1e-6);
        assert!((interval.upper - 0.812_914).abs() < 1e-6);
    }

    #[test]
    fn clopper_pearson_all_successes_supports_stage_2_6_scale() {
        let interval = clopper_pearson(4096, 4096, 0.95).expect("valid interval");
        assert!((interval.lower - 0.999_099_78).abs() < 1e-7);
        assert_eq!(interval.upper, 1.0);
    }

    #[test]
    fn graph_only_peeler_matches_actual_codec_completion() {
        let geometry = FiniteReservoirGeometry::solve(1024, rate(30, 100), rate(5, 100))
            .expect("small finite geometry is representable");
        let graph_seed = 0x5EED;
        let graph = FiniteGraph::build(geometry, graph_seed).expect("graph builds");
        let mut peeler = Peeler::new(&graph).expect("peeler allocates");
        let symbol_bytes = NonZeroUsize::new(8).expect("non-zero symbol size");
        let mut encoder = Encoder::new_terminated(
            geometry.params(),
            symbol_bytes,
            graph_seed,
            geometry.source_count(),
        );
        let mut decoder = Decoder::new_terminated(
            geometry.params(),
            symbol_bytes,
            graph_seed,
            geometry.source_count(),
        );
        let mut bins = Vec::new();
        for source_id in 0..geometry.source_count() {
            bins.extend(encoder.push_source(&source_id.to_le_bytes()));
        }
        bins.extend(encoder.finish());

        for (bin_id, payload) in bins {
            if bin_id % 101 != 0 {
                peeler.push_bin(bin_id as u32).expect("bin is in range");
                decoder.push_bin(bin_id, payload);
            }
        }

        assert_eq!(
            peeler.complete(),
            decoder.next_source_id() == geometry.source_count()
        );
    }

    #[test]
    fn short_and_long_ge_models_have_equal_stationary_loss_but_distinct_bursts() {
        let short = ChannelModel::gilbert_elliott(
            "ge-short",
            rate(1, 1000),
            rate(1, 10),
            rate(1, 1000),
            rate(1, 1),
        )
        .expect("valid short GE model");
        let long = ChannelModel::gilbert_elliott(
            "ge-long",
            rate(1, 10_000),
            rate(1, 100),
            rate(1, 1000),
            rate(1, 1),
        )
        .expect("valid long GE model");

        assert!(
            (short.stationary_erasure_probability() - long.stationary_erasure_probability()).abs()
                < f64::EPSILON
        );
        assert!((short.stationary_erasure_probability() - 0.010_891_089).abs() < 1e-9);
    }

    #[test]
    fn no_loss_case_completes_without_reserve_or_duplicates() {
        let geometry = FiniteReservoirGeometry::solve(1024, rate(30, 100), rate(5, 100))
            .expect("small finite geometry is representable");
        let channel = ChannelModel::bec("no-loss", rate(0, 1)).expect("valid BEC");
        let summary = run_case(geometry, channel, 4, 7).expect("simulation succeeds");

        assert_eq!(summary.initial_completions, 4);
        assert_eq!(summary.final_completions, 4);
        assert_eq!(summary.duplicate_transmissions, 0);
    }
}
