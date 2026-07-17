//! Deterministic ideal-DoF model for pooled cross-tree FEC speedup.
//!
//! This is model-level evidence, not a codec model and not a WAN measurement. Each receiver owns
//! rank buckets capped at their negotiated degrees of freedom. Every successful ideal-coded
//! delivery is innovative until its bucket is full. All protocols on one seed consume the same
//! potential `(tick, tree, receiver)` service and loss trace, including opportunities skipped while
//! a barrier protocol waits for feedback.

#![allow(
    dead_code,
    reason = "the example and integration test intentionally consume different halves of this shared harness"
)]

use std::array;
use std::error::Error;
use std::fmt::{self, Display, Formatter};
use std::fs::{self, File};
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};

pub const SOURCE_DOF: u32 = 8_192;
pub const DEFAULT_SEEDS: usize = 512;
const MAX_RECEIVERS: usize = 8;
const RATE_DENOMINATOR: u8 = 8;
const PERIODIC_EPOCH_TICKS: u64 = 256;
const RANDOM_WALK_EPOCH_TICKS: u64 = 64;
const MAX_SIMULATION_TICK: u64 = 20_000_000;
const BASE_SEED: u64 = 0x504F_4F4C_5F44_4F46;
const PROFILE_SEED_DOMAIN: u64 = 0x5052_4F46_494C_4553;
const CHANNEL_SEED_DOMAIN: u64 = 0x4348_414E_4E45_4C53;
pub const RESEARCH_SCOPE: &str = "MODEL-LEVEL EVIDENCE - ideal DoF - NOT WAN measurement";

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RateProfile {
    Static { ratio: u8 },
    PeriodicAlternation,
    BoundedRandomWalk,
}

impl RateProfile {
    pub const ALL: [Self; 6] = [
        Self::Static { ratio: 1 },
        Self::Static { ratio: 2 },
        Self::Static { ratio: 4 },
        Self::Static { ratio: 8 },
        Self::PeriodicAlternation,
        Self::BoundedRandomWalk,
    ];

    pub const fn name(self) -> &'static str {
        match self {
            Self::Static { ratio: 1 } => "static-1to1",
            Self::Static { ratio: 2 } => "static-2to1",
            Self::Static { ratio: 4 } => "static-4to1",
            Self::Static { ratio: 8 } => "static-8to1",
            Self::Static { .. } => "static-invalid",
            Self::PeriodicAlternation => "periodic-fast-slow-alternation",
            Self::BoundedRandomWalk => "seeded-bounded-random-walk",
        }
    }

    const fn id(self) -> u64 {
        match self {
            Self::Static { ratio } => ratio as u64,
            Self::PeriodicAlternation => 0x100,
            Self::BoundedRandomWalk => 0x200,
        }
    }

    pub const fn static_ratio(self) -> Option<u8> {
        match self {
            Self::Static { ratio } => Some(ratio),
            Self::PeriodicAlternation | Self::BoundedRandomWalk => None,
        }
    }

    fn nominal_weights(self, tree_count: usize) -> Result<Vec<u32>, SimError> {
        validate_tree_count(tree_count)?;
        match self {
            Self::Static { ratio } => {
                if !matches!(ratio, 1 | 2 | 4 | 8) {
                    return Err(SimError::InvalidRateRatio(ratio));
                }
                let slow = u32::from(RATE_DENOMINATOR / ratio);
                let mut weights = vec![slow; tree_count];
                weights[0] = u32::from(RATE_DENOMINATOR);
                Ok(weights)
            }
            Self::PeriodicAlternation | Self::BoundedRandomWalk => Ok(vec![1; tree_count]),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LossModel {
    None,
    BecHalfPercent,
    BecTwoPercent,
    GilbertElliottShort,
    GilbertElliottLong,
}

impl LossModel {
    pub const ALL: [Self; 5] = [
        Self::None,
        Self::BecHalfPercent,
        Self::BecTwoPercent,
        Self::GilbertElliottShort,
        Self::GilbertElliottLong,
    ];

    pub const fn name(self) -> &'static str {
        match self {
            Self::None => "none",
            Self::BecHalfPercent => "bec-0.5pct",
            Self::BecTwoPercent => "bec-2.0pct",
            Self::GilbertElliottShort => "ge-short-1.089pct",
            Self::GilbertElliottLong => "ge-long-1.089pct",
        }
    }

    const fn id(self) -> u64 {
        match self {
            Self::None => 0,
            Self::BecHalfPercent => 1,
            Self::BecTwoPercent => 2,
            Self::GilbertElliottShort => 3,
            Self::GilbertElliottLong => 4,
        }
    }

    pub const fn stationary_erasure(self) -> Rate {
        match self {
            Self::None => Rate::new_const(0, 1),
            Self::BecHalfPercent => Rate::new_const(5, 1_000),
            Self::BecTwoPercent => Rate::new_const(2, 100),
            Self::GilbertElliottShort | Self::GilbertElliottLong => Rate::new_const(11, 1_010),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Protocol {
    EqualSplitStriping,
    RateProportionalStriping,
    PerStripeFec,
    PooledRounds,
    PooledCarousel,
}

impl Protocol {
    pub const ALL: [Self; 5] = [
        Self::EqualSplitStriping,
        Self::RateProportionalStriping,
        Self::PerStripeFec,
        Self::PooledRounds,
        Self::PooledCarousel,
    ];

    pub const fn name(self) -> &'static str {
        match self {
            Self::EqualSplitStriping => "equal_split_striping",
            Self::RateProportionalStriping => "rate_proportional_striping",
            Self::PerStripeFec => "per_stripe_fec",
            Self::PooledRounds => "pooled_rounds",
            Self::PooledCarousel => "pooled_carousel",
        }
    }

    const fn index(self) -> usize {
        match self {
            Self::EqualSplitStriping => 0,
            Self::RateProportionalStriping => 1,
            Self::PerStripeFec => 2,
            Self::PooledRounds => 3,
            Self::PooledCarousel => 4,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TrialMetrics {
    pub receiver_completion_ticks: Vec<u64>,
    pub barrier_completion_tick: u64,
    pub sender_stop_tick: u64,
    pub total_emissions: u64,
    pub ownership_wasted_deliveries: u64,
    pub post_completion_tail_deliveries: u64,
    pub post_completion_tail_emissions: u64,
    pub tree_emissions: Vec<u64>,
    pub tree_available_opportunities: Vec<u64>,
}

#[derive(Debug)]
pub enum SimError {
    ArithmeticOverflow,
    InvalidArguments(String),
    InvalidRateRatio(u8),
    InvalidReceiverCount(usize),
    InvalidTreeCount(usize),
    Io(std::io::Error),
    SimulationDidNotConverge,
    TraceInvariant(&'static str),
    WorkerPanicked,
}

impl Display for SimError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::ArithmeticOverflow => write!(formatter, "simulation arithmetic overflow"),
            Self::InvalidArguments(message) => write!(formatter, "{message}"),
            Self::InvalidRateRatio(ratio) => write!(formatter, "invalid static rate ratio {ratio}"),
            Self::InvalidReceiverCount(count) => {
                write!(
                    formatter,
                    "receiver count {count} is outside 1..={MAX_RECEIVERS}"
                )
            }
            Self::InvalidTreeCount(count) => write!(formatter, "tree count {count} is unsupported"),
            Self::Io(error) => write!(formatter, "experiment I/O failed: {error}"),
            Self::SimulationDidNotConverge => write!(formatter, "simulation did not converge"),
            Self::TraceInvariant(message) => write!(formatter, "trace invariant failed: {message}"),
            Self::WorkerPanicked => write!(formatter, "simulation worker panicked"),
        }
    }
}

impl Error for SimError {}

impl From<std::io::Error> for SimError {
    fn from(error: std::io::Error) -> Self {
        Self::Io(error)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Rate {
    numerator: u64,
    denominator: u64,
}

impl Rate {
    pub const fn new_const(numerator: u64, denominator: u64) -> Self {
        Self {
            numerator,
            denominator,
        }
    }

    const fn as_f64(self) -> f64 {
        self.numerator as f64 / self.denominator as f64
    }
}

#[derive(Clone, Copy)]
struct Event {
    tick: u64,
}

struct OpportunityGenerator {
    profile: RateProfile,
    profile_seed: u64,
    tick: u64,
    accumulators: Vec<u8>,
    random_walk_units: Vec<u8>,
}

impl OpportunityGenerator {
    fn new(profile: RateProfile, profile_seed: u64, tree_count: usize) -> Result<Self, SimError> {
        validate_tree_count(tree_count)?;
        if let RateProfile::Static { ratio } = profile
            && !matches!(ratio, 1 | 2 | 4 | 8)
        {
            return Err(SimError::InvalidRateRatio(ratio));
        }
        Ok(Self {
            profile,
            profile_seed,
            tick: 0,
            accumulators: vec![0; tree_count],
            random_walk_units: vec![4; tree_count],
        })
    }

    fn next_tick(&mut self) -> Result<(u64, u8), SimError> {
        self.tick = self
            .tick
            .checked_add(1)
            .ok_or(SimError::ArithmeticOverflow)?;
        if self.tick > MAX_SIMULATION_TICK {
            return Err(SimError::SimulationDidNotConverge);
        }
        if self.profile == RateProfile::BoundedRandomWalk
            && self.tick > 1
            && (self.tick - 1).is_multiple_of(RANDOM_WALK_EPOCH_TICKS)
        {
            let epoch = (self.tick - 1) / RANDOM_WALK_EPOCH_TICKS;
            for (tree, units) in self.random_walk_units.iter_mut().enumerate() {
                let draw = mix64(
                    self.profile_seed
                        ^ epoch.rotate_left(17)
                        ^ u64::try_from(tree).map_err(|_| SimError::ArithmeticOverflow)?,
                ) % 3;
                *units = match draw {
                    0 => units.saturating_sub(1).max(1),
                    1 => *units,
                    _ => units.saturating_add(1).min(RATE_DENOMINATOR),
                };
            }
        }

        let mut opportunity_mask = 0u8;
        for tree in 0..self.accumulators.len() {
            let units = self.rate_units(tree)?;
            let accumulator = self.accumulators[tree]
                .checked_add(units)
                .ok_or(SimError::ArithmeticOverflow)?;
            if accumulator >= RATE_DENOMINATOR {
                self.accumulators[tree] = accumulator - RATE_DENOMINATOR;
                opportunity_mask |= 1u8
                    .checked_shl(u32::try_from(tree).map_err(|_| SimError::ArithmeticOverflow)?)
                    .ok_or(SimError::ArithmeticOverflow)?;
            } else {
                self.accumulators[tree] = accumulator;
            }
        }
        Ok((self.tick, opportunity_mask))
    }

    fn rate_units(&self, tree: usize) -> Result<u8, SimError> {
        match self.profile {
            RateProfile::Static { ratio } => {
                if tree == 0 {
                    Ok(RATE_DENOMINATOR)
                } else {
                    RATE_DENOMINATOR
                        .checked_div(ratio)
                        .ok_or(SimError::InvalidRateRatio(ratio))
                }
            }
            RateProfile::PeriodicAlternation => {
                let epoch = (self.tick - 1) / PERIODIC_EPOCH_TICKS;
                let fast_tree = usize::try_from(
                    epoch
                        % u64::try_from(self.accumulators.len())
                            .map_err(|_| SimError::ArithmeticOverflow)?,
                )
                .map_err(|_| SimError::ArithmeticOverflow)?;
                Ok(if tree == fast_tree {
                    RATE_DENOMINATOR
                } else {
                    1
                })
            }
            RateProfile::BoundedRandomWalk => Ok(self.random_walk_units[tree]),
        }
    }
}

struct ChannelState {
    model: LossModel,
    rng: SplitMix64,
    in_bad_state: bool,
}

impl ChannelState {
    fn new(model: LossModel, seed: u64) -> Self {
        let mut rng = SplitMix64::new(seed);
        let in_bad_state = match model {
            LossModel::GilbertElliottShort | LossModel::GilbertElliottLong => {
                rng.sample_ratio(1, 101)
            }
            LossModel::None | LossModel::BecHalfPercent | LossModel::BecTwoPercent => false,
        };
        Self {
            model,
            rng,
            in_bad_state,
        }
    }

    fn delivers(&mut self) -> bool {
        match self.model {
            LossModel::None => true,
            LossModel::BecHalfPercent => !self.rng.sample_ratio(5, 1_000),
            LossModel::BecTwoPercent => !self.rng.sample_ratio(2, 100),
            LossModel::GilbertElliottShort => {
                self.gilbert_elliott_delivers(Rate::new_const(1, 1_000), Rate::new_const(1, 10))
            }
            LossModel::GilbertElliottLong => {
                self.gilbert_elliott_delivers(Rate::new_const(1, 10_000), Rate::new_const(1, 100))
            }
        }
    }

    fn gilbert_elliott_delivers(&mut self, good_to_bad: Rate, bad_to_good: Rate) -> bool {
        let erased = if self.in_bad_state {
            true
        } else {
            self.rng.sample_ratio(1, 1_000)
        };
        let transition = if self.in_bad_state {
            self.rng
                .sample_ratio(bad_to_good.numerator, bad_to_good.denominator)
        } else {
            self.rng
                .sample_ratio(good_to_bad.numerator, good_to_bad.denominator)
        };
        if transition {
            self.in_bad_state = !self.in_bad_state;
        }
        !erased
    }
}

struct CoupledTrace {
    events: Vec<Event>,
    global_success_prefix: [Vec<u32>; MAX_RECEIVERS],
    tree_global_indices: Vec<Vec<usize>>,
    tree_success_prefix: Vec<[Vec<u32>; MAX_RECEIVERS]>,
    opportunity_generator: OpportunityGenerator,
    channels: Vec<ChannelState>,
    tree_count: usize,
}

impl CoupledTrace {
    fn new(
        tree_count: usize,
        profile: RateProfile,
        loss: LossModel,
        seed: u64,
    ) -> Result<Self, SimError> {
        validate_tree_count(tree_count)?;
        let mut channels = Vec::with_capacity(tree_count * MAX_RECEIVERS);
        for tree in 0..tree_count {
            for receiver in 0..MAX_RECEIVERS {
                let channel_seed = mix64(
                    seed ^ CHANNEL_SEED_DOMAIN
                        ^ (u64::try_from(tree).map_err(|_| SimError::ArithmeticOverflow)? << 32)
                        ^ u64::try_from(receiver).map_err(|_| SimError::ArithmeticOverflow)?,
                );
                channels.push(ChannelState::new(loss, channel_seed));
            }
        }
        let mut global_success_prefix: [Vec<u32>; MAX_RECEIVERS] = array::from_fn(|_| Vec::new());
        for prefix in &mut global_success_prefix {
            prefix.push(0);
        }
        let mut tree_success_prefix = Vec::with_capacity(tree_count);
        for _ in 0..tree_count {
            let mut prefixes: [Vec<u32>; MAX_RECEIVERS] = array::from_fn(|_| Vec::new());
            for prefix in &mut prefixes {
                prefix.push(0);
            }
            tree_success_prefix.push(prefixes);
        }
        Ok(Self {
            events: Vec::new(),
            global_success_prefix,
            tree_global_indices: vec![Vec::new(); tree_count],
            tree_success_prefix,
            opportunity_generator: OpportunityGenerator::new(
                profile,
                mix64(seed ^ PROFILE_SEED_DOMAIN),
                tree_count,
            )?,
            channels,
            tree_count,
        })
    }

    fn generate_next_tick(&mut self) -> Result<(), SimError> {
        let (tick, mask) = self.opportunity_generator.next_tick()?;
        for tree in 0..self.tree_count {
            if mask & (1 << tree) == 0 {
                continue;
            }
            let mut delivery_mask = 0u8;
            for receiver in 0..MAX_RECEIVERS {
                if self.channels[tree * MAX_RECEIVERS + receiver].delivers() {
                    delivery_mask |= 1 << receiver;
                }
            }
            let global_index = self.events.len();
            self.events.push(Event { tick });
            self.tree_global_indices[tree].push(global_index);
            for receiver in 0..MAX_RECEIVERS {
                let delivered = u32::from(delivery_mask & (1 << receiver) != 0);
                let global_next = self.global_success_prefix[receiver]
                    .last()
                    .copied()
                    .ok_or(SimError::TraceInvariant("empty global prefix"))?
                    .checked_add(delivered)
                    .ok_or(SimError::ArithmeticOverflow)?;
                self.global_success_prefix[receiver].push(global_next);
                let tree_next = self.tree_success_prefix[tree][receiver]
                    .last()
                    .copied()
                    .ok_or(SimError::TraceInvariant("empty tree prefix"))?
                    .checked_add(delivered)
                    .ok_or(SimError::ArithmeticOverflow)?;
                self.tree_success_prefix[tree][receiver].push(tree_next);
            }
        }
        Ok(())
    }

    fn ensure_through_tick(&mut self, tick: u64) -> Result<(), SimError> {
        while self.opportunity_generator.tick < tick {
            self.generate_next_tick()?;
        }
        Ok(())
    }

    fn ensure_global_count(&mut self, count: usize) -> Result<(), SimError> {
        while self.events.len() < count {
            self.generate_next_tick()?;
        }
        Ok(())
    }

    fn ensure_tree_count(&mut self, tree: usize, count: usize) -> Result<(), SimError> {
        while self.tree_global_indices[tree].len() < count {
            self.generate_next_tick()?;
        }
        Ok(())
    }

    fn ensure_global_successes(
        &mut self,
        receiver_count: usize,
        target: u32,
    ) -> Result<(), SimError> {
        validate_receiver_count(receiver_count)?;
        while (0..receiver_count)
            .any(|receiver| self.global_success_prefix[receiver].last().copied() < Some(target))
        {
            self.generate_next_tick()?;
        }
        Ok(())
    }

    fn ensure_tree_successes(
        &mut self,
        receiver_count: usize,
        quotas: &[u32],
    ) -> Result<(), SimError> {
        validate_receiver_count(receiver_count)?;
        while (0..self.tree_count).any(|tree| {
            (0..receiver_count).any(|receiver| {
                self.tree_success_prefix[tree][receiver].last().copied() < Some(quotas[tree])
            })
        }) {
            self.generate_next_tick()?;
        }
        Ok(())
    }

    fn first_global_at_tick(&mut self, tick: u64) -> Result<usize, SimError> {
        self.ensure_through_tick(tick)?;
        Ok(self.events.partition_point(|event| event.tick < tick))
    }

    fn first_tree_at_tick(&mut self, tree: usize, tick: u64) -> Result<usize, SimError> {
        self.ensure_through_tick(tick)?;
        Ok(self.tree_global_indices[tree]
            .partition_point(|global| self.events[*global].tick < tick))
    }

    fn global_successes(&self, receiver: usize, start: usize, end: usize) -> u32 {
        self.global_success_prefix[receiver][end] - self.global_success_prefix[receiver][start]
    }

    fn tree_successes(&self, tree: usize, receiver: usize, start: usize, end: usize) -> u32 {
        self.tree_success_prefix[tree][receiver][end]
            - self.tree_success_prefix[tree][receiver][start]
    }

    fn global_nth_success_event(
        &self,
        receiver: usize,
        start: usize,
        end: usize,
        ordinal: u32,
    ) -> Result<usize, SimError> {
        if ordinal == 0 || self.global_successes(receiver, start, end) < ordinal {
            return Err(SimError::TraceInvariant(
                "global success ordinal outside interval",
            ));
        }
        let target = self.global_success_prefix[receiver][start]
            .checked_add(ordinal)
            .ok_or(SimError::ArithmeticOverflow)?;
        let prefix_index = self.global_success_prefix[receiver][start + 1..=end]
            .partition_point(|value| *value < target)
            + start
            + 1;
        Ok(prefix_index - 1)
    }

    fn tree_nth_success_local(
        &self,
        tree: usize,
        receiver: usize,
        start: usize,
        end: usize,
        ordinal: u32,
    ) -> Result<usize, SimError> {
        if ordinal == 0 || self.tree_successes(tree, receiver, start, end) < ordinal {
            return Err(SimError::TraceInvariant(
                "tree success ordinal outside interval",
            ));
        }
        let target = self.tree_success_prefix[tree][receiver][start]
            .checked_add(ordinal)
            .ok_or(SimError::ArithmeticOverflow)?;
        let prefix_index = self.tree_success_prefix[tree][receiver][start + 1..=end]
            .partition_point(|value| *value < target)
            + start
            + 1;
        Ok(prefix_index - 1)
    }

    fn tree_global_index(&self, tree: usize, local: usize) -> usize {
        self.tree_global_indices[tree][local]
    }

    fn tree_event_count_in_global_interval(&self, tree: usize, start: usize, end: usize) -> usize {
        let indices = &self.tree_global_indices[tree];
        indices.partition_point(|index| *index < end)
            - indices.partition_point(|index| *index < start)
    }

    fn tree_count_before_tick(&mut self, tree: usize, tick: u64) -> Result<usize, SimError> {
        if tick > 0 {
            self.ensure_through_tick(tick - 1)?;
        }
        Ok(self.tree_global_indices[tree]
            .partition_point(|global| self.events[*global].tick < tick))
    }
}

pub fn simulate_protocols(
    tree_count: usize,
    profile: RateProfile,
    loss: LossModel,
    receiver_count: usize,
    feedback_rtt: u64,
    seed: u64,
) -> Result<Vec<(Protocol, TrialMetrics)>, SimError> {
    validate_tree_count(tree_count)?;
    validate_receiver_count(receiver_count)?;
    let equal_quotas = equal_quotas(tree_count)?;
    let proportional_quotas = proportional_quotas(profile, tree_count)?;
    let mut trace = CoupledTrace::new(tree_count, profile, loss, seed)?;
    let equal = simulate_striping_rounds(&mut trace, receiver_count, feedback_rtt, &equal_quotas)?;
    let proportional = simulate_striping_rounds(
        &mut trace,
        receiver_count,
        feedback_rtt,
        &proportional_quotas,
    )?;
    let stripe_fec = simulate_per_stripe_fec(&mut trace, receiver_count, &proportional_quotas)?;
    let rounds = simulate_pooled_rounds(&mut trace, receiver_count, feedback_rtt)?;
    let carousel = simulate_pooled_carousel(&mut trace, receiver_count, feedback_rtt)?;
    Ok(vec![
        (Protocol::EqualSplitStriping, equal),
        (Protocol::RateProportionalStriping, proportional),
        (Protocol::PerStripeFec, stripe_fec),
        (Protocol::PooledRounds, rounds),
        (Protocol::PooledCarousel, carousel),
    ])
}

fn simulate_striping_rounds(
    trace: &mut CoupledTrace,
    receiver_count: usize,
    feedback_rtt: u64,
    quotas: &[u32],
) -> Result<TrialMetrics, SimError> {
    let tree_count = quotas.len();
    let mut ranks = vec![vec![0u32; tree_count]; receiver_count];
    let mut delivered = vec![vec![0u64; tree_count]; receiver_count];
    let mut stripe_completion_event = vec![vec![None; tree_count]; receiver_count];
    let mut batch_counts = quotas.to_vec();
    let mut start_tick = 1u64;
    let mut total_emissions = 0u64;
    let mut tree_emissions = vec![0u64; tree_count];
    let sender_stop_tick;

    loop {
        let mut round_end_tick = 0u64;
        for tree in 0..tree_count {
            let count =
                usize::try_from(batch_counts[tree]).map_err(|_| SimError::ArithmeticOverflow)?;
            if count == 0 {
                continue;
            }
            let start = trace.first_tree_at_tick(tree, start_tick)?;
            let end = start
                .checked_add(count)
                .ok_or(SimError::ArithmeticOverflow)?;
            trace.ensure_tree_count(tree, end)?;
            let last_global = trace.tree_global_index(tree, end - 1);
            round_end_tick = round_end_tick.max(trace.events[last_global].tick);
            total_emissions = total_emissions
                .checked_add(u64::try_from(count).map_err(|_| SimError::ArithmeticOverflow)?)
                .ok_or(SimError::ArithmeticOverflow)?;
            tree_emissions[tree] = tree_emissions[tree]
                .checked_add(u64::try_from(count).map_err(|_| SimError::ArithmeticOverflow)?)
                .ok_or(SimError::ArithmeticOverflow)?;

            for receiver in 0..receiver_count {
                let successes = trace.tree_successes(tree, receiver, start, end);
                delivered[receiver][tree] = delivered[receiver][tree]
                    .checked_add(u64::from(successes))
                    .ok_or(SimError::ArithmeticOverflow)?;
                let deficit = quotas[tree] - ranks[receiver][tree];
                if successes >= deficit && deficit > 0 {
                    let local =
                        trace.tree_nth_success_local(tree, receiver, start, end, deficit)?;
                    stripe_completion_event[receiver][tree] =
                        Some(trace.tree_global_index(tree, local));
                }
                ranks[receiver][tree] = quotas[tree].min(
                    ranks[receiver][tree]
                        .checked_add(successes)
                        .ok_or(SimError::ArithmeticOverflow)?,
                );
            }
        }
        if round_end_tick == 0 {
            return Err(SimError::TraceInvariant("empty incomplete striping round"));
        }
        let feedback_tick = round_end_tick
            .checked_add(feedback_rtt)
            .ok_or(SimError::ArithmeticOverflow)?;
        if ranks.iter().all(|receiver| {
            receiver
                .iter()
                .zip(quotas)
                .all(|(rank, quota)| rank == quota)
        }) {
            sender_stop_tick = feedback_tick;
            break;
        }
        start_tick = feedback_tick;
        for tree in 0..tree_count {
            batch_counts[tree] = (0..receiver_count)
                .map(|receiver| quotas[tree] - ranks[receiver][tree])
                .max()
                .unwrap_or(0);
        }
    }

    let receiver_completion_ticks = stripe_completion_event
        .iter()
        .map(|events| {
            events
                .iter()
                .map(|event| {
                    event
                        .map(|index| trace.events[index].tick)
                        .ok_or(SimError::TraceInvariant("missing stripe completion event"))
                })
                .collect::<Result<Vec<_>, _>>()?
                .into_iter()
                .max()
                .ok_or(SimError::TraceInvariant("receiver has no stripes"))
        })
        .collect::<Result<Vec<_>, SimError>>()?;
    let barrier_completion_tick = receiver_completion_ticks
        .iter()
        .copied()
        .max()
        .ok_or(SimError::TraceInvariant("no receiver completion"))?;
    let ownership_wasted_deliveries = delivered
        .iter()
        .flat_map(|receiver| receiver.iter().zip(quotas))
        .try_fold(0u64, |sum, (successes, quota)| {
            sum.checked_add(successes.saturating_sub(u64::from(*quota)))
                .ok_or(SimError::ArithmeticOverflow)
        })?;
    let tree_available_opportunities = tree_available_before(trace, sender_stop_tick)?;

    Ok(TrialMetrics {
        receiver_completion_ticks,
        barrier_completion_tick,
        sender_stop_tick,
        total_emissions,
        ownership_wasted_deliveries,
        post_completion_tail_deliveries: 0,
        post_completion_tail_emissions: 0,
        tree_emissions,
        tree_available_opportunities,
    })
}

fn simulate_per_stripe_fec(
    trace: &mut CoupledTrace,
    receiver_count: usize,
    quotas: &[u32],
) -> Result<TrialMetrics, SimError> {
    trace.ensure_tree_successes(receiver_count, quotas)?;
    let tree_count = quotas.len();
    let mut receiver_completion_events = Vec::with_capacity(receiver_count);
    for receiver in 0..receiver_count {
        let mut completion_event = 0usize;
        for (tree, quota) in quotas.iter().copied().enumerate() {
            let local = trace.tree_nth_success_local(
                tree,
                receiver,
                0,
                trace.tree_global_indices[tree].len(),
                quota,
            )?;
            completion_event = completion_event.max(trace.tree_global_index(tree, local));
        }
        receiver_completion_events.push(completion_event);
    }
    let last_event = receiver_completion_events
        .iter()
        .copied()
        .max()
        .ok_or(SimError::TraceInvariant("no receiver completion"))?;
    let emitted_end = last_event
        .checked_add(1)
        .ok_or(SimError::ArithmeticOverflow)?;
    let receiver_completion_ticks = receiver_completion_events
        .iter()
        .map(|event| trace.events[*event].tick)
        .collect::<Vec<_>>();
    let barrier_completion_tick = trace.events[last_event].tick;
    let sender_stop_tick = barrier_completion_tick
        .checked_add(1)
        .ok_or(SimError::ArithmeticOverflow)?;
    let mut tree_emissions = Vec::with_capacity(tree_count);
    let mut ownership_wasted_deliveries = 0u64;
    for (tree, quota) in quotas.iter().copied().enumerate() {
        let local_end =
            trace.tree_global_indices[tree].partition_point(|index| *index < emitted_end);
        tree_emissions.push(u64::try_from(local_end).map_err(|_| SimError::ArithmeticOverflow)?);
        for receiver in 0..receiver_count {
            let successes = trace.tree_successes(tree, receiver, 0, local_end);
            ownership_wasted_deliveries = ownership_wasted_deliveries
                .checked_add(u64::from(successes.saturating_sub(quota)))
                .ok_or(SimError::ArithmeticOverflow)?;
        }
    }
    let total_emissions = u64::try_from(emitted_end).map_err(|_| SimError::ArithmeticOverflow)?;

    Ok(TrialMetrics {
        receiver_completion_ticks,
        barrier_completion_tick,
        sender_stop_tick,
        total_emissions,
        ownership_wasted_deliveries,
        post_completion_tail_deliveries: 0,
        post_completion_tail_emissions: 0,
        tree_available_opportunities: tree_emissions.clone(),
        tree_emissions,
    })
}

fn simulate_pooled_rounds(
    trace: &mut CoupledTrace,
    receiver_count: usize,
    feedback_rtt: u64,
) -> Result<TrialMetrics, SimError> {
    let mut ranks = vec![0u32; receiver_count];
    let mut completion_events = vec![None; receiver_count];
    let mut start_tick = 1u64;
    let mut batch_count = SOURCE_DOF;
    let mut total_emissions = 0u64;
    let mut tree_emissions = vec![0u64; trace.tree_count];
    let sender_stop_tick;

    loop {
        let start = trace.first_global_at_tick(start_tick)?;
        let count = usize::try_from(batch_count).map_err(|_| SimError::ArithmeticOverflow)?;
        let end = start
            .checked_add(count)
            .ok_or(SimError::ArithmeticOverflow)?;
        trace.ensure_global_count(end)?;
        for receiver in 0..receiver_count {
            let successes = trace.global_successes(receiver, start, end);
            let deficit = SOURCE_DOF - ranks[receiver];
            if successes >= deficit && deficit > 0 {
                completion_events[receiver] =
                    Some(trace.global_nth_success_event(receiver, start, end, deficit)?);
            }
            ranks[receiver] = SOURCE_DOF.min(
                ranks[receiver]
                    .checked_add(successes)
                    .ok_or(SimError::ArithmeticOverflow)?,
            );
        }
        total_emissions = total_emissions
            .checked_add(u64::from(batch_count))
            .ok_or(SimError::ArithmeticOverflow)?;
        for (tree, emissions) in tree_emissions.iter_mut().enumerate() {
            *emissions = emissions
                .checked_add(
                    u64::try_from(trace.tree_event_count_in_global_interval(tree, start, end))
                        .map_err(|_| SimError::ArithmeticOverflow)?,
                )
                .ok_or(SimError::ArithmeticOverflow)?;
        }
        let round_end_tick = trace.events[end - 1].tick;
        let feedback_tick = round_end_tick
            .checked_add(feedback_rtt)
            .ok_or(SimError::ArithmeticOverflow)?;
        if ranks.iter().all(|rank| *rank == SOURCE_DOF) {
            sender_stop_tick = feedback_tick;
            break;
        }
        batch_count = ranks
            .iter()
            .map(|rank| SOURCE_DOF - *rank)
            .max()
            .unwrap_or(0);
        if batch_count == 0 {
            return Err(SimError::TraceInvariant(
                "incomplete pooled rank has zero deficit",
            ));
        }
        start_tick = feedback_tick;
    }

    let receiver_completion_ticks = completion_events
        .into_iter()
        .map(|event| {
            event
                .map(|index| trace.events[index].tick)
                .ok_or(SimError::TraceInvariant("missing pooled completion event"))
        })
        .collect::<Result<Vec<_>, _>>()?;
    let barrier_completion_tick = receiver_completion_ticks
        .iter()
        .copied()
        .max()
        .ok_or(SimError::TraceInvariant("no pooled receiver completion"))?;
    let tree_available_opportunities = tree_available_before(trace, sender_stop_tick)?;
    Ok(TrialMetrics {
        receiver_completion_ticks,
        barrier_completion_tick,
        sender_stop_tick,
        total_emissions,
        ownership_wasted_deliveries: 0,
        post_completion_tail_deliveries: 0,
        post_completion_tail_emissions: 0,
        tree_emissions,
        tree_available_opportunities,
    })
}

fn simulate_pooled_carousel(
    trace: &mut CoupledTrace,
    receiver_count: usize,
    feedback_rtt: u64,
) -> Result<TrialMetrics, SimError> {
    trace.ensure_global_successes(receiver_count, SOURCE_DOF)?;
    let mut completion_events = Vec::with_capacity(receiver_count);
    let mut receiver_completion_ticks = Vec::with_capacity(receiver_count);
    for receiver in 0..receiver_count {
        let event = trace.global_nth_success_event(receiver, 0, trace.events.len(), SOURCE_DOF)?;
        completion_events.push(event);
        receiver_completion_ticks.push(trace.events[event].tick);
    }
    let barrier_completion_tick = receiver_completion_ticks
        .iter()
        .copied()
        .max()
        .ok_or(SimError::TraceInvariant("no carousel receiver completion"))?;
    let sender_stop_tick = barrier_completion_tick
        .checked_add(feedback_rtt)
        .ok_or(SimError::ArithmeticOverflow)?;
    let emitted_end = trace.first_global_at_tick(sender_stop_tick)?;
    let barrier_event = completion_events
        .iter()
        .copied()
        .max()
        .ok_or(SimError::TraceInvariant("no carousel completion event"))?;
    let post_completion_tail_emissions = u64::try_from(
        emitted_end
            .checked_sub(
                barrier_event
                    .checked_add(1)
                    .ok_or(SimError::ArithmeticOverflow)?,
            )
            .ok_or(SimError::TraceInvariant(
                "carousel emission end precedes completion",
            ))?,
    )
    .map_err(|_| SimError::ArithmeticOverflow)?;
    let mut post_completion_tail_deliveries = 0u64;
    for (receiver, completion) in completion_events.into_iter().enumerate() {
        let tail_start = completion
            .checked_add(1)
            .ok_or(SimError::ArithmeticOverflow)?;
        post_completion_tail_deliveries = post_completion_tail_deliveries
            .checked_add(u64::from(trace.global_successes(
                receiver,
                tail_start,
                emitted_end,
            )))
            .ok_or(SimError::ArithmeticOverflow)?;
    }
    let tree_emissions = (0..trace.tree_count)
        .map(|tree| {
            u64::try_from(
                trace.tree_global_indices[tree].partition_point(|index| *index < emitted_end),
            )
            .map_err(|_| SimError::ArithmeticOverflow)
        })
        .collect::<Result<Vec<_>, _>>()?;
    let total_emissions = u64::try_from(emitted_end).map_err(|_| SimError::ArithmeticOverflow)?;
    Ok(TrialMetrics {
        receiver_completion_ticks,
        barrier_completion_tick,
        sender_stop_tick,
        total_emissions,
        ownership_wasted_deliveries: 0,
        post_completion_tail_deliveries,
        post_completion_tail_emissions,
        tree_available_opportunities: tree_emissions.clone(),
        tree_emissions,
    })
}

fn tree_available_before(trace: &mut CoupledTrace, tick: u64) -> Result<Vec<u64>, SimError> {
    (0..trace.tree_count)
        .map(|tree| {
            u64::try_from(trace.tree_count_before_tick(tree, tick)?)
                .map_err(|_| SimError::ArithmeticOverflow)
        })
        .collect()
}

fn equal_quotas(tree_count: usize) -> Result<Vec<u32>, SimError> {
    validate_tree_count(tree_count)?;
    let tree_count_u32 = u32::try_from(tree_count).map_err(|_| SimError::ArithmeticOverflow)?;
    let base = SOURCE_DOF / tree_count_u32;
    let remainder = SOURCE_DOF % tree_count_u32;
    (0..tree_count)
        .map(|tree| {
            Ok(base
                + u32::from(
                    u32::try_from(tree).map_err(|_| SimError::ArithmeticOverflow)? < remainder,
                ))
        })
        .collect()
}

fn proportional_quotas(profile: RateProfile, tree_count: usize) -> Result<Vec<u32>, SimError> {
    let weights = profile.nominal_weights(tree_count)?;
    let total_weight = weights.iter().try_fold(0u64, |sum, weight| {
        sum.checked_add(u64::from(*weight))
            .ok_or(SimError::ArithmeticOverflow)
    })?;
    let mut quotas = Vec::with_capacity(tree_count);
    let mut remainders = Vec::with_capacity(tree_count);
    let mut assigned = 0u32;
    for (tree, weight) in weights.into_iter().enumerate() {
        let scaled = u64::from(SOURCE_DOF)
            .checked_mul(u64::from(weight))
            .ok_or(SimError::ArithmeticOverflow)?;
        let quota =
            u32::try_from(scaled / total_weight).map_err(|_| SimError::ArithmeticOverflow)?;
        assigned = assigned
            .checked_add(quota)
            .ok_or(SimError::ArithmeticOverflow)?;
        quotas.push(quota);
        remainders.push((scaled % total_weight, tree));
    }
    remainders.sort_unstable_by(|left, right| right.0.cmp(&left.0).then(left.1.cmp(&right.1)));
    for (_, tree) in remainders
        .into_iter()
        .take(usize::try_from(SOURCE_DOF - assigned).map_err(|_| SimError::ArithmeticOverflow)?)
    {
        quotas[tree] = quotas[tree]
            .checked_add(1)
            .ok_or(SimError::ArithmeticOverflow)?;
    }
    if quotas.iter().copied().sum::<u32>() != SOURCE_DOF {
        return Err(SimError::TraceInvariant(
            "quota apportionment does not sum to K",
        ));
    }
    Ok(quotas)
}

fn validate_tree_count(tree_count: usize) -> Result<(), SimError> {
    if matches!(tree_count, 2 | 4 | 8) {
        Ok(())
    } else {
        Err(SimError::InvalidTreeCount(tree_count))
    }
}

fn validate_receiver_count(receiver_count: usize) -> Result<(), SimError> {
    if (1..=MAX_RECEIVERS).contains(&receiver_count) {
        Ok(())
    } else {
        Err(SimError::InvalidReceiverCount(receiver_count))
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
        mix64(self.state)
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

fn mix64(mut value: u64) -> u64 {
    value ^= value >> 30;
    value = value.wrapping_mul(0xBF58_476D_1CE4_E5B9);
    value ^= value >> 27;
    value = value.wrapping_mul(0x94D0_49BB_1331_11EB);
    value ^ (value >> 31)
}

// Sweep aggregation and CSV output live below the model so the example remains a standalone,
// dependency-free executable while integration tests can exercise exactly the same implementation.

#[derive(Default)]
struct MetricSamples {
    receiver_ticks: Vec<Vec<u64>>,
    barrier_ticks: Vec<u64>,
    sender_stop_ticks: Vec<u64>,
    emissions: Vec<u64>,
    ownership_waste: Vec<u64>,
    carousel_tail_deliveries: Vec<u64>,
    carousel_tail_emissions: Vec<u64>,
    tree_emissions_sum: Vec<u128>,
    tree_available_sum: Vec<u128>,
}

impl MetricSamples {
    fn new(receiver_count: usize, tree_count: usize, seeds: usize) -> Self {
        Self {
            receiver_ticks: (0..receiver_count)
                .map(|_| Vec::with_capacity(seeds))
                .collect(),
            barrier_ticks: Vec::with_capacity(seeds),
            sender_stop_ticks: Vec::with_capacity(seeds),
            emissions: Vec::with_capacity(seeds),
            ownership_waste: Vec::with_capacity(seeds),
            carousel_tail_deliveries: Vec::with_capacity(seeds),
            carousel_tail_emissions: Vec::with_capacity(seeds),
            tree_emissions_sum: vec![0; tree_count],
            tree_available_sum: vec![0; tree_count],
        }
    }

    fn push(&mut self, metrics: &TrialMetrics) -> Result<(), SimError> {
        if self.receiver_ticks.len() != metrics.receiver_completion_ticks.len()
            || self.tree_emissions_sum.len() != metrics.tree_emissions.len()
            || self.tree_available_sum.len() != metrics.tree_available_opportunities.len()
        {
            return Err(SimError::TraceInvariant("aggregate metric shape mismatch"));
        }
        for (samples, tick) in self
            .receiver_ticks
            .iter_mut()
            .zip(&metrics.receiver_completion_ticks)
        {
            samples.push(*tick);
        }
        self.barrier_ticks.push(metrics.barrier_completion_tick);
        self.sender_stop_ticks.push(metrics.sender_stop_tick);
        self.emissions.push(metrics.total_emissions);
        self.ownership_waste
            .push(metrics.ownership_wasted_deliveries);
        self.carousel_tail_deliveries
            .push(metrics.post_completion_tail_deliveries);
        self.carousel_tail_emissions
            .push(metrics.post_completion_tail_emissions);
        for (sum, value) in self
            .tree_emissions_sum
            .iter_mut()
            .zip(&metrics.tree_emissions)
        {
            *sum = sum
                .checked_add(u128::from(*value))
                .ok_or(SimError::ArithmeticOverflow)?;
        }
        for (sum, value) in self
            .tree_available_sum
            .iter_mut()
            .zip(&metrics.tree_available_opportunities)
        {
            *sum = sum
                .checked_add(u128::from(*value))
                .ok_or(SimError::ArithmeticOverflow)?;
        }
        Ok(())
    }
}

#[derive(Default)]
struct DecompositionSamples {
    stripe_gap: Vec<i64>,
    stripe_waste: Vec<u64>,
    stripe_emission_gap: Vec<i64>,
    rounds_gap: Vec<i64>,
    rounds_emission_gap: Vec<i64>,
    carousel_tail_deliveries: Vec<u64>,
    carousel_tail_emissions: Vec<u64>,
    carousel_emissions: Vec<u64>,
}

impl DecompositionSamples {
    fn with_capacity(seeds: usize) -> Self {
        Self {
            stripe_gap: Vec::with_capacity(seeds),
            stripe_waste: Vec::with_capacity(seeds),
            stripe_emission_gap: Vec::with_capacity(seeds),
            rounds_gap: Vec::with_capacity(seeds),
            rounds_emission_gap: Vec::with_capacity(seeds),
            carousel_tail_deliveries: Vec::with_capacity(seeds),
            carousel_tail_emissions: Vec::with_capacity(seeds),
            carousel_emissions: Vec::with_capacity(seeds),
        }
    }

    fn push(
        &mut self,
        stripe: &TrialMetrics,
        rounds: &TrialMetrics,
        carousel: &TrialMetrics,
    ) -> Result<(), SimError> {
        self.stripe_gap.push(signed_difference(
            stripe.barrier_completion_tick,
            carousel.barrier_completion_tick,
        )?);
        self.stripe_waste.push(stripe.ownership_wasted_deliveries);
        self.stripe_emission_gap.push(signed_difference(
            stripe.total_emissions,
            carousel.total_emissions,
        )?);
        self.rounds_gap.push(signed_difference(
            rounds.barrier_completion_tick,
            carousel.barrier_completion_tick,
        )?);
        self.rounds_emission_gap.push(signed_difference(
            rounds.total_emissions,
            carousel.total_emissions,
        )?);
        self.carousel_tail_deliveries
            .push(carousel.post_completion_tail_deliveries);
        self.carousel_tail_emissions
            .push(carousel.post_completion_tail_emissions);
        self.carousel_emissions.push(carousel.total_emissions);
        Ok(())
    }
}

struct BaseSeedOutcome {
    cells: Vec<Vec<TrialMetrics>>,
}

fn simulate_base_seed(
    tree_count: usize,
    profile: RateProfile,
    loss: LossModel,
    trial_seed: u64,
) -> Result<BaseSeedOutcome, SimError> {
    let equal_quotas = equal_quotas(tree_count)?;
    let proportional_quotas = proportional_quotas(profile, tree_count)?;
    let mut trace = CoupledTrace::new(tree_count, profile, loss, trial_seed)?;
    let mut cells = Vec::with_capacity(3 * 3);
    for receiver_count in [1usize, 3, 8] {
        let stripe_fec = simulate_per_stripe_fec(&mut trace, receiver_count, &proportional_quotas)?;
        for feedback_rtt in [8u64, 64, 512] {
            let equal =
                simulate_striping_rounds(&mut trace, receiver_count, feedback_rtt, &equal_quotas)?;
            let proportional = simulate_striping_rounds(
                &mut trace,
                receiver_count,
                feedback_rtt,
                &proportional_quotas,
            )?;
            let rounds = simulate_pooled_rounds(&mut trace, receiver_count, feedback_rtt)?;
            let carousel = simulate_pooled_carousel(&mut trace, receiver_count, feedback_rtt)?;
            cells.push(vec![
                equal,
                proportional,
                stripe_fec.clone(),
                rounds,
                carousel,
            ]);
        }
    }
    Ok(BaseSeedOutcome { cells })
}

struct PredictionAccumulator {
    homogeneous_gap: Vec<i64>,
    p_b_cell_means: Vec<(f64, f64, f64)>,
    p_c_cell_means: Vec<(f64, f64)>,
    tail_fraction_by_rtt: [Vec<f64>; 3],
}

impl PredictionAccumulator {
    fn new() -> Self {
        Self {
            homogeneous_gap: Vec::new(),
            p_b_cell_means: Vec::new(),
            p_c_cell_means: Vec::new(),
            tail_fraction_by_rtt: array::from_fn(|_| Vec::new()),
        }
    }
}

struct CsvOutputs {
    summary: BufWriter<File>,
    receivers: BufWriter<File>,
    trees: BufWriter<File>,
    decomposition: BufWriter<File>,
    predictions: BufWriter<File>,
}

impl CsvOutputs {
    fn create(output_dir: &Path) -> Result<Self, SimError> {
        fs::create_dir_all(output_dir)?;
        let mut outputs = Self {
            summary: BufWriter::new(File::create(output_dir.join("summary.csv"))?),
            receivers: BufWriter::new(File::create(output_dir.join("receivers.csv"))?),
            trees: BufWriter::new(File::create(output_dir.join("tree-utilization.csv"))?),
            decomposition: BufWriter::new(File::create(output_dir.join("decomposition.csv"))?),
            predictions: BufWriter::new(File::create(output_dir.join("prediction-tests.csv"))?),
        };
        writeln!(
            outputs.summary,
            "research_scope,source_dof,seeds,trees,rate_profile,static_ratio,loss,loss_num,loss_den,receivers,feedback_rtt_ticks,protocol,receiver_observations,receiver_completion_ticks_sum,mean_receiver_completion_ticks,p95_receiver_completion_ticks,barrier_completion_ticks_sum,mean_barrier_completion_ticks,p95_barrier_completion_ticks,sender_stop_ticks_sum,mean_sender_stop_ticks,p95_sender_stop_ticks,total_emissions_sum,mean_total_emissions,p95_total_emissions,ownership_wasted_deliveries_sum,mean_ownership_wasted_deliveries,p95_ownership_wasted_deliveries,post_completion_tail_deliveries_sum,mean_post_completion_tail_deliveries,p95_post_completion_tail_deliveries,post_completion_tail_emissions_sum,mean_post_completion_tail_emissions,p95_post_completion_tail_emissions,opportunity_weighted_tree_utilization"
        )?;
        writeln!(
            outputs.receivers,
            "research_scope,source_dof,seeds,trees,rate_profile,loss,receivers,feedback_rtt_ticks,protocol,receiver_index,completion_ticks_sum,mean_completion_ticks,p95_completion_ticks"
        )?;
        writeln!(
            outputs.trees,
            "research_scope,source_dof,seeds,trees,rate_profile,loss,receivers,feedback_rtt_ticks,protocol,tree_index,emissions_sum,available_opportunities_sum,utilization"
        )?;
        writeln!(
            outputs.decomposition,
            "research_scope,source_dof,seeds,trees,rate_profile,loss,loss_num,loss_den,receivers,feedback_rtt_ticks,striping_minus_pooling_ticks_sum,mean_striping_minus_pooling_ticks,p95_striping_minus_pooling_ticks,per_stripe_ownership_waste_sum,mean_per_stripe_ownership_waste,p95_per_stripe_ownership_waste,pearson_gap_vs_ownership_waste,per_stripe_minus_carousel_emissions_sum,mean_per_stripe_minus_carousel_emissions,pearson_gap_vs_emission_gap,rounds_minus_carousel_ticks_sum,mean_rounds_minus_carousel_ticks,p95_rounds_minus_carousel_ticks,rounds_minus_carousel_emissions_sum,mean_rounds_minus_carousel_emissions,carousel_tail_deliveries_sum,mean_carousel_tail_deliveries,carousel_tail_emissions_sum,mean_carousel_tail_emissions,mean_carousel_tail_emission_fraction"
        )?;
        writeln!(
            outputs.predictions,
            "research_scope,prediction,statistic,observations,value,secondary_value,interpretation"
        )?;
        Ok(outputs)
    }
}

pub fn run_sweep(seeds: usize, output_dir: &Path) -> Result<(), SimError> {
    if seeds == 0 {
        return Err(SimError::InvalidArguments(
            "seed count must be non-zero".to_owned(),
        ));
    }
    let mut outputs = CsvOutputs::create(output_dir)?;
    let mut predictions = PredictionAccumulator::new();
    for tree_count in [2usize, 4, 8] {
        for profile in RateProfile::ALL {
            for loss in LossModel::ALL {
                let outcomes = run_base_seeds(tree_count, profile, loss, seeds)?;
                write_base_results(
                    &mut outputs,
                    &mut predictions,
                    tree_count,
                    profile,
                    loss,
                    seeds,
                    &outcomes,
                )?;
            }
        }
    }
    write_prediction_results(&mut outputs.predictions, &predictions)?;
    Ok(())
}

fn run_base_seeds(
    tree_count: usize,
    profile: RateProfile,
    loss: LossModel,
    seeds: usize,
) -> Result<Vec<BaseSeedOutcome>, SimError> {
    let worker_count = std::thread::available_parallelism()
        .map_or(1, std::num::NonZeroUsize::get)
        .min(seeds);
    let chunk = seeds.div_ceil(worker_count);
    let chunks = std::thread::scope(|scope| {
        let mut handles = Vec::new();
        for worker in 0..worker_count {
            let start = worker * chunk;
            let end = seeds.min(start + chunk);
            if start >= end {
                continue;
            }
            handles.push(scope.spawn(move || {
                let mut outcomes = Vec::with_capacity(end - start);
                for trial in start..end {
                    let trial_seed = trial_seed(tree_count, profile, loss, trial)?;
                    outcomes.push((
                        trial,
                        simulate_base_seed(tree_count, profile, loss, trial_seed)?,
                    ));
                }
                Ok::<_, SimError>(outcomes)
            }));
        }
        handles
            .into_iter()
            .map(|handle| handle.join().map_err(|_| SimError::WorkerPanicked)?)
            .collect::<Result<Vec<_>, SimError>>()
    })?;
    let mut indexed = chunks.into_iter().flatten().collect::<Vec<_>>();
    indexed.sort_unstable_by_key(|(trial, _)| *trial);
    Ok(indexed.into_iter().map(|(_, outcome)| outcome).collect())
}

#[allow(clippy::too_many_arguments)]
fn write_base_results(
    outputs: &mut CsvOutputs,
    predictions: &mut PredictionAccumulator,
    tree_count: usize,
    profile: RateProfile,
    loss: LossModel,
    seeds: usize,
    outcomes: &[BaseSeedOutcome],
) -> Result<(), SimError> {
    let receiver_counts = [1usize, 3, 8];
    let feedback_rtts = [8u64, 64, 512];
    for (receiver_index, receiver_count) in receiver_counts.into_iter().enumerate() {
        for (rtt_index, feedback_rtt) in feedback_rtts.into_iter().enumerate() {
            let cell_index = receiver_index * feedback_rtts.len() + rtt_index;
            let mut protocol_samples =
                Protocol::ALL.map(|_| MetricSamples::new(receiver_count, tree_count, seeds));
            let mut decomposition = DecompositionSamples::with_capacity(seeds);
            for outcome in outcomes {
                let metrics = &outcome.cells[cell_index];
                for protocol in Protocol::ALL {
                    protocol_samples[protocol.index()].push(&metrics[protocol.index()])?;
                }
                decomposition.push(
                    &metrics[Protocol::PerStripeFec.index()],
                    &metrics[Protocol::PooledRounds.index()],
                    &metrics[Protocol::PooledCarousel.index()],
                )?;
                if profile == (RateProfile::Static { ratio: 1 })
                    && loss == LossModel::None
                    && feedback_rtt == 64
                {
                    predictions.homogeneous_gap.push(signed_difference(
                        metrics[Protocol::RateProportionalStriping.index()].barrier_completion_tick,
                        metrics[Protocol::PooledCarousel.index()].barrier_completion_tick,
                    )?);
                }
            }
            for protocol in Protocol::ALL {
                write_metric_rows(
                    outputs,
                    tree_count,
                    profile,
                    loss,
                    receiver_count,
                    feedback_rtt,
                    protocol,
                    &protocol_samples[protocol.index()],
                )?;
            }
            write_decomposition_row(
                &mut outputs.decomposition,
                tree_count,
                profile,
                loss,
                receiver_count,
                feedback_rtt,
                &decomposition,
            )?;

            if feedback_rtt == 64 {
                predictions.p_b_cell_means.push((
                    mean_i64(&decomposition.stripe_gap),
                    mean_u64(&decomposition.stripe_waste),
                    mean_i64(&decomposition.stripe_emission_gap),
                ));
            }
            let erasure = loss.stationary_erasure().as_f64();
            predictions.p_c_cell_means.push((
                feedback_rtt as f64 * erasure,
                mean_i64(&decomposition.rounds_gap),
            ));
            let mean_carousel_emissions = mean_u64(&decomposition.carousel_emissions);
            let tail_fraction = if mean_carousel_emissions == 0.0 {
                0.0
            } else {
                mean_u64(&decomposition.carousel_tail_emissions) / mean_carousel_emissions
            };
            predictions.tail_fraction_by_rtt[rtt_index].push(tail_fraction);
        }
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn write_metric_rows(
    outputs: &mut CsvOutputs,
    tree_count: usize,
    profile: RateProfile,
    loss: LossModel,
    receiver_count: usize,
    feedback_rtt: u64,
    protocol: Protocol,
    samples: &MetricSamples,
) -> Result<(), SimError> {
    let receiver_observations = samples.receiver_ticks.iter().map(Vec::len).sum::<usize>();
    let receiver_sum = samples
        .receiver_ticks
        .iter()
        .flat_map(|values| values.iter())
        .try_fold(0u128, |sum, value| {
            sum.checked_add(u128::from(*value))
                .ok_or(SimError::ArithmeticOverflow)
        })?;
    let receiver_flat = samples
        .receiver_ticks
        .iter()
        .flat_map(|values| values.iter().copied())
        .collect::<Vec<_>>();
    let tree_emission_total = samples.tree_emissions_sum.iter().sum::<u128>();
    let tree_available_total = samples.tree_available_sum.iter().sum::<u128>();
    let utilization = ratio_u128(tree_emission_total, tree_available_total);
    let loss_rate = loss.stationary_erasure();
    writeln!(
        outputs.summary,
        "{RESEARCH_SCOPE},{SOURCE_DOF},{},{tree_count},{},{},{},{},{},{receiver_count},{feedback_rtt},{},{receiver_observations},{receiver_sum},{:.6},{},{},{:.6},{},{},{:.6},{},{},{:.6},{},{},{:.6},{},{},{:.6},{},{},{:.6},{},{utilization:.9}",
        samples.barrier_ticks.len(),
        profile.name(),
        profile
            .static_ratio()
            .map_or_else(String::new, |ratio| ratio.to_string()),
        loss.name(),
        loss_rate.numerator,
        loss_rate.denominator,
        protocol.name(),
        receiver_sum as f64 / receiver_observations as f64,
        percentile_u64(&receiver_flat, 95),
        sum_u64(&samples.barrier_ticks)?,
        mean_u64(&samples.barrier_ticks),
        percentile_u64(&samples.barrier_ticks, 95),
        sum_u64(&samples.sender_stop_ticks)?,
        mean_u64(&samples.sender_stop_ticks),
        percentile_u64(&samples.sender_stop_ticks, 95),
        sum_u64(&samples.emissions)?,
        mean_u64(&samples.emissions),
        percentile_u64(&samples.emissions, 95),
        sum_u64(&samples.ownership_waste)?,
        mean_u64(&samples.ownership_waste),
        percentile_u64(&samples.ownership_waste, 95),
        sum_u64(&samples.carousel_tail_deliveries)?,
        mean_u64(&samples.carousel_tail_deliveries),
        percentile_u64(&samples.carousel_tail_deliveries, 95),
        sum_u64(&samples.carousel_tail_emissions)?,
        mean_u64(&samples.carousel_tail_emissions),
        percentile_u64(&samples.carousel_tail_emissions, 95),
    )?;

    for (receiver, values) in samples.receiver_ticks.iter().enumerate() {
        writeln!(
            outputs.receivers,
            "{RESEARCH_SCOPE},{SOURCE_DOF},{},{tree_count},{},{},{receiver_count},{feedback_rtt},{},{receiver},{},{:.6},{}",
            values.len(),
            profile.name(),
            loss.name(),
            protocol.name(),
            sum_u64(values)?,
            mean_u64(values),
            percentile_u64(values, 95),
        )?;
    }
    for tree in 0..tree_count {
        writeln!(
            outputs.trees,
            "{RESEARCH_SCOPE},{SOURCE_DOF},{},{tree_count},{},{},{receiver_count},{feedback_rtt},{},{tree},{},{},{:.9}",
            samples.barrier_ticks.len(),
            profile.name(),
            loss.name(),
            protocol.name(),
            samples.tree_emissions_sum[tree],
            samples.tree_available_sum[tree],
            ratio_u128(
                samples.tree_emissions_sum[tree],
                samples.tree_available_sum[tree]
            ),
        )?;
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn write_decomposition_row(
    writer: &mut impl Write,
    tree_count: usize,
    profile: RateProfile,
    loss: LossModel,
    receiver_count: usize,
    feedback_rtt: u64,
    samples: &DecompositionSamples,
) -> Result<(), SimError> {
    let loss_rate = loss.stationary_erasure();
    let mean_carousel_emissions = mean_u64(&samples.carousel_emissions);
    let tail_fraction = if mean_carousel_emissions == 0.0 {
        0.0
    } else {
        mean_u64(&samples.carousel_tail_emissions) / mean_carousel_emissions
    };
    writeln!(
        writer,
        "{RESEARCH_SCOPE},{SOURCE_DOF},{},{tree_count},{},{},{},{},{receiver_count},{feedback_rtt},{},{:.6},{},{},{:.6},{},{},{},{:.6},{},{},{:.6},{},{},{:.6},{},{:.6},{},{:.6},{tail_fraction:.9}",
        samples.stripe_gap.len(),
        profile.name(),
        loss.name(),
        loss_rate.numerator,
        loss_rate.denominator,
        sum_i64(&samples.stripe_gap)?,
        mean_i64(&samples.stripe_gap),
        percentile_i64(&samples.stripe_gap, 95),
        sum_u64(&samples.stripe_waste)?,
        mean_u64(&samples.stripe_waste),
        percentile_u64(&samples.stripe_waste, 95),
        format_optional(pearson_i64_u64(&samples.stripe_gap, &samples.stripe_waste)),
        sum_i64(&samples.stripe_emission_gap)?,
        mean_i64(&samples.stripe_emission_gap),
        format_optional(pearson_i64_i64(
            &samples.stripe_gap,
            &samples.stripe_emission_gap
        )),
        sum_i64(&samples.rounds_gap)?,
        mean_i64(&samples.rounds_gap),
        percentile_i64(&samples.rounds_gap, 95),
        sum_i64(&samples.rounds_emission_gap)?,
        mean_i64(&samples.rounds_emission_gap),
        sum_u64(&samples.carousel_tail_deliveries)?,
        mean_u64(&samples.carousel_tail_deliveries),
        sum_u64(&samples.carousel_tail_emissions)?,
        mean_u64(&samples.carousel_tail_emissions),
    )?;
    Ok(())
}

fn write_prediction_results(
    writer: &mut impl Write,
    predictions: &PredictionAccumulator,
) -> Result<(), SimError> {
    let max_abs_gap = predictions
        .homogeneous_gap
        .iter()
        .map(|value| value.unsigned_abs())
        .max()
        .unwrap_or(0);
    writeln!(
        writer,
        "{RESEARCH_SCOPE},P-a,homogeneous_no_loss_rate_proportional_gap,{},\"{:.6}\",{},mean ticks and maximum absolute tick gap",
        predictions.homogeneous_gap.len(),
        mean_i64(&predictions.homogeneous_gap),
        max_abs_gap,
    )?;

    let gap = predictions
        .p_b_cell_means
        .iter()
        .map(|values| values.0)
        .collect::<Vec<_>>();
    let waste = predictions
        .p_b_cell_means
        .iter()
        .map(|values| values.1)
        .collect::<Vec<_>>();
    let emissions = predictions
        .p_b_cell_means
        .iter()
        .map(|values| values.2)
        .collect::<Vec<_>>();
    writeln!(
        writer,
        "{RESEARCH_SCOPE},P-b,cell_mean_gap_correlations,{},\"{:.9}\",\"{:.9}\",Pearson gap-vs-ownership waste then gap-vs-emission gap at RTT 64",
        gap.len(),
        pearson_f64(&gap, &waste).unwrap_or(0.0),
        pearson_f64(&gap, &emissions).unwrap_or(0.0),
    )?;

    let predictor = predictions
        .p_c_cell_means
        .iter()
        .map(|values| values.0)
        .collect::<Vec<_>>();
    let rounds_gap = predictions
        .p_c_cell_means
        .iter()
        .map(|values| values.1)
        .collect::<Vec<_>>();
    writeln!(
        writer,
        "{RESEARCH_SCOPE},P-c,rounds_gap_vs_rtt_times_stationary_loss,{},\"{:.9}\",,Pearson correlation across cell means",
        predictor.len(),
        pearson_f64(&predictor, &rounds_gap).unwrap_or(0.0),
    )?;
    for (index, rtt) in [8u64, 64, 512].into_iter().enumerate() {
        let values = &predictions.tail_fraction_by_rtt[index];
        writeln!(
            writer,
            "{RESEARCH_SCOPE},P-c,carousel_tail_emission_fraction_at_rtt_{rtt},{},\"{:.9}\",\"{:.9}\",mean sender tail-emission fraction then coefficient of variation across cells",
            values.len(),
            mean_f64(values),
            coefficient_of_variation(values),
        )?;
    }
    Ok(())
}

fn trial_seed(
    tree_count: usize,
    profile: RateProfile,
    loss: LossModel,
    trial: usize,
) -> Result<u64, SimError> {
    Ok(mix64(
        BASE_SEED
            ^ (u64::try_from(tree_count).map_err(|_| SimError::ArithmeticOverflow)? << 56)
            ^ (profile.id() << 32)
            ^ (loss.id() << 24)
            ^ u64::try_from(trial).map_err(|_| SimError::ArithmeticOverflow)?,
    ))
}

fn signed_difference(lhs: u64, rhs: u64) -> Result<i64, SimError> {
    let difference = i128::from(lhs) - i128::from(rhs);
    i64::try_from(difference).map_err(|_| SimError::ArithmeticOverflow)
}

fn sum_u64(values: &[u64]) -> Result<u128, SimError> {
    values.iter().try_fold(0u128, |sum, value| {
        sum.checked_add(u128::from(*value))
            .ok_or(SimError::ArithmeticOverflow)
    })
}

fn sum_i64(values: &[i64]) -> Result<i128, SimError> {
    values.iter().try_fold(0i128, |sum, value| {
        sum.checked_add(i128::from(*value))
            .ok_or(SimError::ArithmeticOverflow)
    })
}

fn mean_u64(values: &[u64]) -> f64 {
    values.iter().map(|value| *value as f64).sum::<f64>() / values.len() as f64
}

fn mean_i64(values: &[i64]) -> f64 {
    values.iter().map(|value| *value as f64).sum::<f64>() / values.len() as f64
}

fn mean_f64(values: &[f64]) -> f64 {
    values.iter().sum::<f64>() / values.len() as f64
}

fn percentile_u64(values: &[u64], percentile: usize) -> u64 {
    let mut sorted = values.to_vec();
    sorted.sort_unstable();
    sorted[percentile_index(sorted.len(), percentile)]
}

fn percentile_i64(values: &[i64], percentile: usize) -> i64 {
    let mut sorted = values.to_vec();
    sorted.sort_unstable();
    sorted[percentile_index(sorted.len(), percentile)]
}

fn percentile_index(len: usize, percentile: usize) -> usize {
    (len * percentile)
        .div_ceil(100)
        .saturating_sub(1)
        .min(len - 1)
}

fn ratio_u128(numerator: u128, denominator: u128) -> f64 {
    if denominator == 0 {
        0.0
    } else {
        numerator as f64 / denominator as f64
    }
}

fn pearson_i64_u64(left: &[i64], right: &[u64]) -> Option<f64> {
    let left = left.iter().map(|value| *value as f64).collect::<Vec<_>>();
    let right = right.iter().map(|value| *value as f64).collect::<Vec<_>>();
    pearson_f64(&left, &right)
}

fn pearson_i64_i64(left: &[i64], right: &[i64]) -> Option<f64> {
    let left = left.iter().map(|value| *value as f64).collect::<Vec<_>>();
    let right = right.iter().map(|value| *value as f64).collect::<Vec<_>>();
    pearson_f64(&left, &right)
}

fn pearson_f64(left: &[f64], right: &[f64]) -> Option<f64> {
    if left.len() != right.len() || left.len() < 2 {
        return None;
    }
    let left_mean = mean_f64(left);
    let right_mean = mean_f64(right);
    let mut covariance = 0.0;
    let mut left_variance = 0.0;
    let mut right_variance = 0.0;
    for (left_value, right_value) in left.iter().zip(right) {
        let left_delta = *left_value - left_mean;
        let right_delta = *right_value - right_mean;
        covariance += left_delta * right_delta;
        left_variance += left_delta * left_delta;
        right_variance += right_delta * right_delta;
    }
    let denominator = (left_variance * right_variance).sqrt();
    (denominator > 0.0).then_some(covariance / denominator)
}

fn coefficient_of_variation(values: &[f64]) -> f64 {
    let mean = mean_f64(values);
    if mean == 0.0 {
        return 0.0;
    }
    let variance = values
        .iter()
        .map(|value| (*value - mean).powi(2))
        .sum::<f64>()
        / values.len() as f64;
    variance.sqrt() / mean
}

fn format_optional(value: Option<f64>) -> String {
    value.map_or_else(String::new, |value| format!("{value:.9}"))
}

pub fn run_cli(arguments: impl IntoIterator<Item = String>) -> Result<(), SimError> {
    let arguments = arguments.into_iter().collect::<Vec<_>>();
    match arguments.as_slice() {
        [mode, seeds, output_dir] if mode == "sweep" => {
            let seeds = seeds.parse::<usize>().map_err(|error| {
                SimError::InvalidArguments(format!("invalid seed count `{seeds}`: {error}"))
            })?;
            run_sweep(seeds, Path::new(output_dir))
        }
        [mode] if mode == "default-sweep" => {
            run_sweep(DEFAULT_SEEDS, Path::new("results/speedup-sim"))
        }
        _ => Err(SimError::InvalidArguments(
            "usage: pooling_speedup_sim sweep <seeds> <output-dir>\n       pooling_speedup_sim default-sweep"
                .to_owned(),
        )),
    }
}

pub fn default_output_dir() -> PathBuf {
    PathBuf::from("results/speedup-sim")
}
